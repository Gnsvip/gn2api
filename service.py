"""Independent ModelTrace monitor with opt-in native account scheduling control."""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import random
import re
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlsplit

import psycopg
import requests
from urllib3.exceptions import ReadTimeoutError
from flask import Flask, jsonify, request
from psycopg.rows import dict_row
from waitress import serve

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "vendor"))
from fingerprint import analyze_global_outputs, generate_challenges, load_bank, parse_numbers

BANK_COMMIT = "6cd4a8ff0f3f4e49d6d6969dd2fbadfa1569fde2"
BANK = load_bank(ROOT / "vendor/data/unified_bank.json")
MODELS = {m["id"]: m for m in BANK["models"]}
INTERVAL = 600
MAX_HISTORY = 10
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
PROMPT_MAX_CHARS = 4000
DEFAULT_PROMPT = "{{challenge}}"
PROBE_READ_TIMEOUT = 120
CONFIDENCE = 0.80  # Conservative display threshold, not a guarantee of identity.
MARGIN = 0.25
LOG = logging.getLogger("modeltrace")


class ProbeError(Exception):
    """Only sanitized, user-readable messages may enter this exception."""


class InsufficientSample(ProbeError):
    """A complete response with too few numbers; a new challenge may replace it."""


class Cancelled(Exception):
    pass


class Store:
    def __init__(self, path):
        self.path = str(path)
        self.action_lock = threading.RLock()
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.db() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS monitors (
                    account_id INTEGER PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 0,
                    model TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 1,
                    next_run REAL NOT NULL DEFAULT 0, running INTEGER NOT NULL DEFAULT 0,
                    updated_at REAL NOT NULL, message TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL,
                    checked_at REAL NOT NULL, model TEXT NOT NULL, result TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS results_account ON results(account_id, id);
                CREATE TABLE IF NOT EXISTS prompt_monitors (
                    account_id INTEGER PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 0,
                    model TEXT NOT NULL DEFAULT '', prompt TEXT NOT NULL DEFAULT '',
                    generation INTEGER NOT NULL DEFAULT 1,
                    next_run REAL NOT NULL DEFAULT 0, running INTEGER NOT NULL DEFAULT 0,
                    updated_at REAL NOT NULL, message TEXT NOT NULL DEFAULT '',
                    auto_pause_enabled INTEGER NOT NULL DEFAULT 0,
                    auto_pause_threshold INTEGER NOT NULL DEFAULT 3,
                    failure_streak INTEGER NOT NULL DEFAULT 0,
                    failure_context TEXT NOT NULL DEFAULT '', pause_status TEXT NOT NULL DEFAULT '',
                    pause_retry_at REAL NOT NULL DEFAULT 0, paused_at REAL NOT NULL DEFAULT 0,
                    pause_message TEXT NOT NULL DEFAULT '', run_sequence INTEGER NOT NULL DEFAULT 0,
                    interval_minutes INTEGER NOT NULL DEFAULT 10,
                    challenge_count INTEGER NOT NULL DEFAULT 3
                );
                CREATE TABLE IF NOT EXISTS prompt_results (
                    id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL,
                    checked_at REAL NOT NULL, model TEXT NOT NULL, result TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS prompt_results_account ON prompt_results(account_id, id);
            """)
            # Migrate existing installations without losing settings or history.
            columns = {row[1] for row in db.execute("PRAGMA table_info(monitors)")}
            for name, definition in {
                "auto_pause_enabled": "INTEGER NOT NULL DEFAULT 0",
                "auto_pause_threshold": "INTEGER NOT NULL DEFAULT 3",
                "mismatch_streak": "INTEGER NOT NULL DEFAULT 0",
                "mismatch_context": "TEXT NOT NULL DEFAULT ''",
                "pause_status": "TEXT NOT NULL DEFAULT ''",
                "pause_retry_at": "REAL NOT NULL DEFAULT 0",
                "paused_at": "REAL NOT NULL DEFAULT 0",
                "pause_message": "TEXT NOT NULL DEFAULT ''",
                "run_sequence": "INTEGER NOT NULL DEFAULT 0",
                "interval_minutes": "INTEGER NOT NULL DEFAULT 10",
                "challenge_count": "INTEGER NOT NULL DEFAULT 3",
            }.items():
                if name not in columns:
                    db.execute(f"ALTER TABLE monitors ADD COLUMN {name} {definition}")

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def get(self, account_id):
        with self.db() as db:
            row = db.execute("SELECT * FROM monitors WHERE account_id=?", (account_id,)).fetchone()
            return dict(row) if row else None

    def get_prompt(self, account_id):
        with self.db() as db:
            row = db.execute("SELECT * FROM prompt_monitors WHERE account_id=?", (account_id,)).fetchone()
            return dict(row) if row else None

    def configure_prompt(self, account_id, enabled, model, prompt,
                         auto_pause_enabled=None, auto_pause_threshold=None, interval_minutes=None,
                         challenge_count=None):
        with self.action_lock, self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM prompt_monitors WHERE account_id=?", (account_id,)).fetchone()
            auto_pause_enabled = bool(old and old["auto_pause_enabled"]) if auto_pause_enabled is None else auto_pause_enabled
            auto_pause_threshold = (old["auto_pause_threshold"] if old else 3) if auto_pause_threshold is None else auto_pause_threshold
            interval_minutes = (old["interval_minutes"] if old else 10) if interval_minutes is None else interval_minutes
            challenge_count = (old["challenge_count"] if old else 3) if challenge_count is None else challenge_count
            same = (old and bool(old["enabled"]) == enabled and old["model"] == model
                    and old["prompt"] == prompt and old["challenge_count"] == challenge_count
                    and bool(old["auto_pause_enabled"]) == auto_pause_enabled
                    and old["auto_pause_threshold"] == auto_pause_threshold)
            if same:
                if old["interval_minutes"] != interval_minutes:
                    db.execute("UPDATE prompt_monitors SET interval_minutes=?,next_run=?,updated_at=? WHERE account_id=?",
                               (interval_minutes, time.time() + interval_minutes * 60, time.time(), account_id))
                return
            same_monitor = old and bool(old["enabled"]) == enabled
            next_run = (old["next_run"] if old["interval_minutes"] == interval_minutes
                        else time.time() + interval_minutes * 60) if same_monitor else time.time() + random.uniform(1, 12)
            db.execute("""INSERT INTO prompt_monitors(account_id,enabled,model,prompt,next_run,updated_at,
                    auto_pause_enabled,auto_pause_threshold,interval_minutes,challenge_count)
                VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id) DO UPDATE SET
                enabled=excluded.enabled, model=excluded.model, prompt=excluded.prompt,
                challenge_count=excluded.challenge_count, generation=generation+1,
                next_run=excluded.next_run, updated_at=excluded.updated_at,
                auto_pause_enabled=excluded.auto_pause_enabled,
                auto_pause_threshold=excluded.auto_pause_threshold,
                interval_minutes=excluded.interval_minutes,
                failure_streak=0,failure_context='',pause_status='',pause_retry_at=0,
                paused_at=0,pause_message='',running=0,message=''""",
                (account_id, int(enabled), model, prompt, next_run, time.time(),
                 int(auto_pause_enabled), auto_pause_threshold, interval_minutes, challenge_count))

    def configure(self, account_id, enabled, model, auto_pause_enabled=None, auto_pause_threshold=None,
                  interval_minutes=None, challenge_count=None):
        with self.action_lock, self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM monitors WHERE account_id=?", (account_id,)).fetchone()
            auto_pause_enabled = bool(old and old["auto_pause_enabled"]) if auto_pause_enabled is None else auto_pause_enabled
            auto_pause_threshold = (old["auto_pause_threshold"] if old else 3) if auto_pause_threshold is None else auto_pause_threshold
            interval_minutes = (old["interval_minutes"] if old else 10) if interval_minutes is None else interval_minutes
            challenge_count = (old["challenge_count"] if old else 3) if challenge_count is None else challenge_count
            # Repeated identical requests must not schedule extra paid probes.
            if (old and bool(old["enabled"]) == enabled and old["model"] == model
                    and bool(old["auto_pause_enabled"]) == auto_pause_enabled
                    and old["auto_pause_threshold"] == auto_pause_threshold
                    and old["challenge_count"] == challenge_count):
                if old["interval_minutes"] != interval_minutes:
                    # A timing-only edit must not cancel an in-flight round or reset red counts.
                    db.execute("UPDATE monitors SET interval_minutes=?,next_run=?,updated_at=? WHERE account_id=?",
                               (interval_minutes, time.time() + interval_minutes * 60, time.time(), account_id))
                return
            same_monitor = old and bool(old["enabled"]) == enabled and old["model"] == model
            next_run = (old["next_run"] if old["interval_minutes"] == interval_minutes
                        else time.time() + interval_minutes * 60) if same_monitor else time.time() + random.uniform(1, 12)
            db.execute("""INSERT INTO monitors(account_id,enabled,model,next_run,updated_at,
                    auto_pause_enabled,auto_pause_threshold,interval_minutes,challenge_count)
                VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id) DO UPDATE SET
                enabled=excluded.enabled, model=excluded.model, generation=generation+1,
                next_run=excluded.next_run, updated_at=excluded.updated_at,
                auto_pause_enabled=excluded.auto_pause_enabled,
                auto_pause_threshold=excluded.auto_pause_threshold,
                interval_minutes=excluded.interval_minutes,
                challenge_count=excluded.challenge_count,
                mismatch_streak=0,mismatch_context='',pause_status='',pause_retry_at=0,
                running=0, message=''""",
                (account_id, int(enabled), model, next_run, time.time(),
                 int(auto_pause_enabled), auto_pause_threshold, interval_minutes, challenge_count))

    def is_current(self, job):
        current = self.get(job["account_id"])
        return bool(current and current["enabled"] and current["generation"] == job["generation"])

    def is_prompt_current(self, job):
        current = self.get_prompt(job["account_id"])
        return bool(current and current["enabled"] and current["generation"] == job["generation"])

    def claim(self):
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM monitors WHERE enabled=1 AND running=0 AND pause_status='' AND next_run<=? "
                             "ORDER BY next_run LIMIT 1", (time.time(),)).fetchone()
            if not row:
                return None
            db.execute("UPDATE monitors SET running=1,run_sequence=run_sequence+1,next_run=?,message='检测中' WHERE account_id=?",
                       (time.time() + row["interval_minutes"] * 60, row["account_id"]))
            return {**dict(row), "run_sequence": row["run_sequence"] + 1}

    def claim_prompt(self):
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("""SELECT * FROM prompt_monitors WHERE enabled=1 AND running=0
                AND pause_status='' AND next_run<=? ORDER BY next_run LIMIT 1""", (time.time(),)).fetchone()
            if not row:
                return None
            db.execute("""UPDATE prompt_monitors SET running=1,run_sequence=run_sequence+1,
                next_run=?,message='提示词测试中' WHERE account_id=?""",
                       (time.time() + row["interval_minutes"] * 60, row["account_id"]))
            return {**dict(row), "run_sequence": row["run_sequence"] + 1}

    def finish(self, job, result):
        with self.action_lock, self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute("SELECT * FROM monitors WHERE account_id=?", (job["account_id"],)).fetchone()
            if (not current or not current["enabled"] or not current["running"]
                    or current["generation"] != job["generation"]
                    or current["run_sequence"] != job["run_sequence"]):
                return
            now = time.time()
            result = dict(result)
            context = result.pop("_context", "")
            streak = 0
            if current["auto_pause_enabled"] and result.get("status") == "red" and context:
                streak = (current["mismatch_streak"] if current["mismatch_context"] == context else 0) + 1
            pending = streak >= current["auto_pause_threshold"]
            result.update(checked_at=now, expected=job["model"], bank_commit=BANK_COMMIT)
            result["mismatch_streak"] = streak
            db.execute("INSERT INTO results(account_id,checked_at,model,result) VALUES(?,?,?,?)",
                       (job["account_id"], now, job["model"], json.dumps(result, ensure_ascii=False)))
            db.execute("DELETE FROM results WHERE account_id=? AND id NOT IN "
                       "(SELECT id FROM results WHERE account_id=? ORDER BY id DESC LIMIT ?)",
                       (job["account_id"], job["account_id"], MAX_HISTORY))
            db.execute("""UPDATE monitors SET running=0,message=?,mismatch_streak=?,mismatch_context=?,
                       pause_status=?,pause_retry_at=0 WHERE account_id=?""",
                       (result.get("message", ""), streak, context if streak else "",
                        "pending" if pending else "", job["account_id"]))

    def finish_prompt(self, job, result):
        with self.action_lock, self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute("SELECT * FROM prompt_monitors WHERE account_id=?", (job["account_id"],)).fetchone()
            if (not current or not current["enabled"] or not current["running"]
                    or current["generation"] != job["generation"]
                    or current["run_sequence"] != job["run_sequence"]):
                return
            now = time.time()
            result = dict(result)
            context = result.pop("_context", "")
            streak = 0
            if current["auto_pause_enabled"] and result.get("status") == "red" and context:
                streak = (current["failure_streak"] if current["failure_context"] == context else 0) + 1
            pending = streak >= current["auto_pause_threshold"]
            result.update(checked_at=now, expected=job["model"], bank_commit=BANK_COMMIT,
                          template_hash=hashlib.sha256(job["prompt"].encode()).hexdigest())
            result["failure_streak"] = streak
            db.execute("INSERT INTO prompt_results(account_id,checked_at,model,result) VALUES(?,?,?,?)",
                       (job["account_id"], now, job["model"], json.dumps(result, ensure_ascii=False)))
            db.execute("DELETE FROM prompt_results WHERE account_id=? AND id NOT IN "
                       "(SELECT id FROM prompt_results WHERE account_id=? ORDER BY id DESC LIMIT ?)",
                       (job["account_id"], job["account_id"], MAX_HISTORY))
            db.execute("""UPDATE prompt_monitors SET running=0,message=?,failure_streak=?,failure_context=?,
                       pause_status=?,pause_retry_at=0 WHERE account_id=?""",
                       (result.get("message", ""), streak, context if streak else "",
                        "pending" if pending else "", job["account_id"]))

    def apply_pending_pause(self, accounts, controller):
        # Serialize with configuration writes: a cancelled generation must never send a pause.
        # Keep the bounded native call inside this transaction so its intent survives crashes.
        with self.action_lock, self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("""SELECT * FROM monitors WHERE enabled=1 AND auto_pause_enabled=1
                AND pause_status IN ('pending','failed') AND pause_retry_at<=?
                AND mismatch_streak>=auto_pause_threshold ORDER BY pause_retry_at LIMIT 1""",
                (time.time(),)).fetchone()
            if not row:
                return
            try:
                account = accounts.get(row["account_id"])
                if account_context(account).hex() != row["mismatch_context"]:
                    db.execute("""UPDATE monitors SET mismatch_streak=0,mismatch_context='',pause_status='',
                        message='账号配置已变化，已取消自动停调度',pause_retry_at=0 WHERE account_id=?""",
                        (row["account_id"],))
                    return
                if account["schedulable"]:
                    controller.pause(row["account_id"])
            except Exception as exc:
                # Never include an upstream response, credential or raw exception in state/logs.
                LOG.warning("pause failed account_id=%s exception=%s", row["account_id"], type(exc).__name__)
                db.execute("""UPDATE monitors SET pause_status='failed',pause_retry_at=?,
                    message='停止调度未确认，将在 60 秒后重试；关闭开关可取消' WHERE account_id=?""",
                    (time.time() + 60, row["account_id"]))
                return
            message = f"连续 {row['mismatch_streak']} 轮复测仍不符，已停止此账号调度；恢复需手动操作"
            db.execute("""UPDATE monitors SET pause_status='paused',paused_at=?,pause_message=?,message=?,
                enabled=0,running=0,generation=generation+1 WHERE account_id=?""",
                (time.time(), message, message, row["account_id"]))

    def apply_pending_prompt_pause(self, accounts, controller):
        with self.action_lock, self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("""SELECT * FROM prompt_monitors WHERE enabled=1 AND auto_pause_enabled=1
                AND pause_status IN ('pending','failed') AND pause_retry_at<=?
                AND failure_streak>=auto_pause_threshold ORDER BY pause_retry_at LIMIT 1""",
                (time.time(),)).fetchone()
            if not row:
                return
            try:
                account = accounts.get(row["account_id"])
                if account_context(account).hex() != row["failure_context"]:
                    db.execute("""UPDATE prompt_monitors SET failure_streak=0,failure_context='',pause_status='',
                        message='账号配置已变化，已取消提示词测试自动停调度',pause_retry_at=0 WHERE account_id=?""",
                        (row["account_id"],))
                    return
                if account["schedulable"]:
                    controller.pause(row["account_id"])
            except Exception as exc:
                LOG.warning("prompt pause failed account_id=%s exception=%s", row["account_id"], type(exc).__name__)
                db.execute("""UPDATE prompt_monitors SET pause_status='failed',pause_retry_at=?,
                    message='提示词测试停止调度未确认，将在 60 秒后重试；关闭开关可取消' WHERE account_id=?""",
                    (time.time() + 60, row["account_id"]))
                return
            message = f"连续 {row['failure_streak']} 轮提示词归因复测仍不符，已停止此账号调度；恢复需手动操作"
            db.execute("""UPDATE prompt_monitors SET pause_status='paused',paused_at=?,pause_message=?,message=?,
                enabled=0,running=0,generation=generation+1 WHERE account_id=?""",
                (time.time(), message, message, row["account_id"]))

    def recover(self):
        with self.db() as db:
            db.execute("""UPDATE monitors SET running=0,mismatch_streak=0,mismatch_context='',
                message='上轮中断，连续计数清零，等待下一轮' WHERE running=1""")
            db.execute("""UPDATE prompt_monitors SET running=0,failure_streak=0,failure_context='',
                message='上轮提示词测试中断，连续计数清零，等待下一轮' WHERE running=1""")

    def public(self, account_id):
        config = self.get(account_id)
        with self.db() as db:
            rows = db.execute("SELECT result FROM results WHERE account_id=? ORDER BY id DESC LIMIT ?",
                              (account_id, MAX_HISTORY)).fetchall()
        prompt = self.get_prompt(account_id)
        with self.db() as db:
            prompt_rows = db.execute("SELECT result FROM prompt_results WHERE account_id=? ORDER BY id DESC LIMIT ?",
                                     (account_id, MAX_HISTORY)).fetchall()
        prompt_public = {
            "enabled": bool(prompt and prompt["enabled"]),
            "model": prompt["model"] if prompt else "",
            "prompt": prompt["prompt"] if prompt else DEFAULT_PROMPT,
            "challenge_count": prompt["challenge_count"] if prompt else 3,
            "running": bool(prompt and prompt["running"]),
            "next_run": prompt["next_run"] if prompt and prompt["enabled"] else None,
            "interval_minutes": prompt["interval_minutes"] if prompt else 10,
            "message": prompt["message"] if prompt else "",
            "auto_pause_enabled": bool(prompt and prompt["auto_pause_enabled"]),
            "auto_pause_threshold": prompt["auto_pause_threshold"] if prompt else 3,
            "failure_streak": prompt["failure_streak"] if prompt else 0,
            "pause_status": prompt["pause_status"] if prompt else "",
            "paused_at": prompt["paused_at"] if prompt else None,
            "pause_message": prompt["pause_message"] if prompt else "",
            "history": [json.loads(r[0]) for r in reversed(prompt_rows)],
        }
        return {
            "account_id": account_id, "enabled": bool(config and config["enabled"]),
            "model": config["model"] if config else "", "running": bool(config and config["running"]),
            "next_run": config["next_run"] if config and config["enabled"] else None,
            "interval_minutes": config["interval_minutes"] if config else 10,
            "challenge_count": config["challenge_count"] if config else 3,
            "interval": config["interval_minutes"] * 60 if config else INTERVAL,
            "message": config["message"] if config else "",
            "auto_pause_enabled": bool(config and config["auto_pause_enabled"]),
            "auto_pause_threshold": config["auto_pause_threshold"] if config else 3,
            "mismatch_streak": config["mismatch_streak"] if config else 0,
            "pause_status": config["pause_status"] if config else "",
            "paused_at": config["paused_at"] if config else None,
            "pause_message": config["pause_message"] if config else "",
            "history": [json.loads(r[0]) for r in reversed(rows)],
            "prompt_test": prompt_public,
        }


class Accounts:
    """PostgreSQL peer-authenticated role with SELECT-only column grants."""
    def __init__(self, dsn):
        self.dsn = dsn

    def query(self, sql, params=()):
        with psycopg.connect(self.dsn, connect_timeout=3, row_factory=dict_row,
                            options="-c statement_timeout=3000 -c default_transaction_read_only=on") as db:
            return db.execute(sql, params).fetchall()

    def metadata(self, ids):
        if not ids:
            return {}
        rows = self.query("""SELECT id,platform,type,status,schedulable,parent_account_id,
            credentials->'model_mapping' AS mapping,
            credentials->>'auth_mode' AS auth_mode
            FROM accounts WHERE id=ANY(%s) AND deleted_at IS NULL""", (ids,))
        return {r["id"]: r for r in rows}

    def get(self, account_id):
        # Refresh tokens, passwords, email and unrelated credentials are not retrieved.
        rows = self.query("""SELECT a.id,a.platform,a.type,a.status,a.schedulable,a.parent_account_id,
            a.proxy_id,a.rate_limit_reset_at,a.overload_until,a.temp_unschedulable_until,a.expires_at,
            jsonb_build_object(
              'api_key',a.credentials->'api_key','access_token',a.credentials->'access_token',
              'base_url',a.credentials->'base_url','model_mapping',a.credentials->'model_mapping',
              'chatgpt_account_id',a.credentials->'chatgpt_account_id',
              'account_id',a.credentials->'account_id','expires_at',a.credentials->'expires_at',
              'user_agent',a.credentials->'user_agent','auth_mode',a.credentials->'auth_mode',
              'header_overrides',a.credentials->'header_overrides') AS credentials,
            jsonb_build_object('openai_responses_mode',a.extra->'openai_responses_mode',
              'openai_responses_supported',a.extra->'openai_responses_supported') AS extra,
            p.protocol AS proxy_protocol,p.host AS proxy_host,p.port AS proxy_port,
            p.username AS proxy_username,p.password AS proxy_password,p.status AS proxy_status,
            p.expires_at AS proxy_expires_at
            FROM accounts a LEFT JOIN proxies p ON a.proxy_id=p.id AND p.deleted_at IS NULL
            WHERE a.id=%s AND a.deleted_at IS NULL""", (account_id,))
        if not rows:
            raise ProbeError("账号不存在或已删除")
        return rows[0]


def mapped_model(model, mapping):
    mapping = mapping or {}
    if model in mapping:
        return mapping[model] or model
    for pattern in sorted(mapping, key=len, reverse=True):
        if "*" in pattern and re.fullmatch(re.escape(pattern).replace(r"\*", ".*"), model):
            return mapping[pattern] or model
    return model


def model_choices(meta):
    family = "gpt" if meta["platform"] == "openai" else "claude"
    mapping = meta.get("mapping") or {}
    return [name for name, entry in MODELS.items() if entry.get("family") == family and (
        not mapping or any(re.fullmatch(re.escape(k).replace(r"\*", ".*"), name) for k in mapping))]


def unavailable(meta):
    if meta["platform"] not in ("openai", "anthropic"):
        return "指纹库暂未支持此平台"
    if meta["type"] not in ("apikey", "oauth") or (meta["platform"] == "anthropic" and meta["type"] != "apikey"):
        return "暂未适配此账号类型"
    if meta.get("parent_account_id") or meta.get("auth_mode") == "agent_identity":
        return "此特殊认证账号暂未适配"
    if meta["status"] != "active" or not meta.get("schedulable"):
        return "账号当前未启用或不可调度"
    if not model_choices(meta):
        return "账号模型尚未被指纹库收录"
    return ""


def prompt_unavailable(meta):
    return unavailable(meta)


def validate_prompt_template(prompt):
    if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > PROMPT_MAX_CHARS:
        raise ProbeError(f"提示词模板不能为空且不能超过 {PROMPT_MAX_CHARS} 个字符")
    if prompt.count("{{challenge}}") != 1:
        raise ProbeError("提示词模板必须包含且仅包含一个 {{challenge}}，用于插入数字挑战")


def epoch(value):
    if not value:
        return 0
    if isinstance(value, (int, float)):
        return value / 1000 if value > 1e12 else value
    if isinstance(value, datetime):
        return value.timestamp()
    try:
        return float(value)
    except (ValueError, TypeError):
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except ValueError:
            return 0


def ensure_available(account):
    c = account["credentials"]
    reason = unavailable({**account, "mapping": c.get("model_mapping"), "auth_mode": c.get("auth_mode")})
    if reason:
        raise ProbeError(reason)
    for field in ("rate_limit_reset_at", "overload_until", "temp_unschedulable_until"):
        if epoch(account.get(field)) > time.time():
            raise ProbeError("账号正在限流或冷却，本轮跳过")
    if account.get("expires_at") and epoch(account["expires_at"]) <= time.time():
        raise ProbeError("账号已过期，本轮跳过")
    if account["type"] == "oauth" and epoch(c.get("expires_at")) and epoch(c["expires_at"]) < time.time() + 60:
        raise ProbeError("等待 sub2api 更新登录令牌，本轮跳过")
    # Usage-window percentages do not include remaining credits. Let the upstream
    # decide whether this request is allowed; keep actual cooldowns/limits above.


def account_context(account):
    """Catch routing/key changes mid-round; ordinary OAuth token refresh is allowed."""
    c = account["credentials"]
    parts = [account["id"], account["platform"], account["type"], c.get("base_url"),
             c.get("api_key"), c.get("model_mapping"), c.get("chatgpt_account_id"),
             c.get("account_id"), c.get("header_overrides"), account.get("extra"),
             account.get("proxy_id"), account.get("proxy_host"), account.get("proxy_port")]
    # Quota snapshots may legitimately change while probes run.
    parts[9] = {key: (account.get("extra") or {}).get(key) for key in
                ("openai_responses_mode", "openai_responses_supported")}
    return hashlib.sha256(json.dumps(parts, sort_keys=True, default=str).encode()).digest()


def proxy_url(a):
    if not a.get("proxy_id"):
        return None
    if not a.get("proxy_host") or a.get("proxy_status") != "active":
        raise ProbeError("账号代理不可用，本轮跳过")
    if a.get("proxy_expires_at") and epoch(a["proxy_expires_at"]) <= time.time():
        raise ProbeError("账号代理已过期，本轮跳过")
    proto = a["proxy_protocol"]
    proto = "socks5h" if proto == "socks5" else proto
    if proto not in ("http", "https", "socks5h"):
        raise ProbeError("暂不支持此代理协议")
    auth = ""
    if a.get("proxy_username"):
        auth = quote(a["proxy_username"], safe="") + ":" + quote(a.get("proxy_password") or "", safe="") + "@"
    host = a["proxy_host"]
    if ":" in host and not host.startswith("["):
        host = "[" + host + "]"
    return f"{proto}://{auth}{host}:{int(a['proxy_port'])}"


def build_request(account, model, prompt, codex_version):
    c = account["credentials"]
    extra = account.get("extra") or {}
    actual_model = mapped_model(model, c.get("model_mapping"))
    if not isinstance(actual_model, str) or len(actual_model) > 160:
        raise ProbeError("模型映射无效")
    headers = {"Content-Type": "application/json", "Accept": "text/event-stream"}
    if account["platform"] == "anthropic":
        protocol = "anthropic"
        endpoint = "messages"
        base = c.get("base_url") or "https://api.anthropic.com"
        headers.update({"x-api-key": c.get("api_key") or "", "anthropic-version": "2023-06-01"})
        payload = {"model": actual_model, "max_tokens": 2048, "temperature": 1,
                   "stream": True, "messages": [{"role": "user", "content": prompt}]}
    elif account["type"] == "oauth":
        protocol = "responses"
        base, endpoint = "https://chatgpt.com/backend-api/codex", "responses"
        headers["Authorization"] = "Bearer " + (c.get("access_token") or "")
        ua = c.get("user_agent") or f"codex_cli_rs/{codex_version} (Linux 6.8.0; x86_64)"
        ua = re.sub(r"^([^/ ]+)/[^ ]+", lambda m: m[1] + "/" + codex_version, ua)
        headers.update({"User-Agent": ua, "originator": ua.split("/", 1)[0],
                        "version": codex_version, "OpenAI-Beta": "responses=experimental"})
        account_header = c.get("chatgpt_account_id") or c.get("account_id")
        if account_header:
            headers["ChatGPT-Account-Id"] = account_header
        # OAuth rejects max_output_tokens. Stream/time/byte bounds below stop runaway output.
        payload = {"model": actual_model, "store": False, "stream": True,
                   "instructions": "Follow the user's instructions exactly. Do not use tools.",
                   "reasoning": {"effort": "low"},
                   "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}]}
    else:
        mode = extra.get("openai_responses_mode")
        use_responses = mode == "force_responses" or (mode != "force_chat_completions" and extra.get("openai_responses_supported") is not False)
        protocol = "responses" if use_responses else "chat"
        endpoint = "responses" if use_responses else "chat/completions"
        base = c.get("base_url") or "https://api.openai.com"
        headers["Authorization"] = "Bearer " + (c.get("api_key") or "")
        if use_responses:
            payload = {"model": actual_model, "stream": True, "max_output_tokens": 4096,
                       "input": [{"role": "user", "content": prompt}]}
        else:
            payload = {"model": actual_model, "stream": True, "max_completion_tokens": 4096,
                       "messages": [{"role": "user", "content": prompt}]}
    parts = urlsplit(base)
    if parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password or parts.query or parts.fragment:
        raise ProbeError("账号接口地址格式不支持")
    base = base.rstrip("/")
    if account["type"] != "oauth" and not re.search(r"/v\d+(?:beta\d*)?$", parts.path.rstrip("/")):
        base += "/v1"
    if not (c.get("access_token") if account["type"] == "oauth" else c.get("api_key")):
        raise ProbeError("账号凭据缺失")
    # Explicit, administrator-configured headers only; no arbitrary client headers.
    for key, value in (c.get("header_overrides") or {}).items():
        if isinstance(value, str) and key.lower() not in ("host", "content-length", "transfer-encoding", "authorization", "x-api-key"):
            if "\r" not in key + value and "\n" not in key + value:
                headers[key] = value
    return base + "/" + endpoint, headers, payload, protocol, actual_model


def stream_failure_message(event):
    # Only show fixed descriptions for recognized codes, never echo raw upstream errors.
    body = event.get("response") if isinstance(event.get("response"), dict) else {}
    error = event.get("error") or body.get("error")
    error = error if isinstance(error, dict) else {}
    code = error.get("code") or event.get("code")
    known = {
        "server_is_overloaded": "上游服务繁忙",
        "server_error": "上游内部错误",
        "rate_limit_exceeded": "上游请求限流",
        "usage_limit_reached": "上游返回用量限制",
        "insufficient_quota": "上游返回额度不足",
        "insufficient_credits": "上游返回点数不足",
    }
    if isinstance(code, str) and code in known:
        return f"{known[code]}（{code}），本轮未完成"
    details = body.get("incomplete_details")
    if isinstance(details, dict) and details.get("reason") == "max_output_tokens":
        return "上游输出达到长度上限，本轮未完成"
    return "上游流内报错，本轮未完成"


def read_stream(response, protocol, cancelled, deadline):
    pieces, received, complete = [], 0, False
    for raw in response.iter_lines(chunk_size=256):
        if cancelled():
            raise Cancelled()
        if time.monotonic() > deadline:
            raise ProbeError("检测超时，本轮未完成")
        received += len(raw)
        if received > MAX_RESPONSE_BYTES:
            raise ProbeError("检测输出超出限制")
        if not raw.startswith(b"data:"):
            continue
        data = raw[5:].strip()
        if data == b"[DONE]":
            if protocol == "chat":
                complete = True
                break
            continue
        try:
            event = json.loads(data)
        except (ValueError, UnicodeDecodeError):
            raise ProbeError("上游返回了无法解析的流式数据")
        if event.get("error") or event.get("type") in ("error", "response.failed", "response.incomplete"):
            raise ProbeError(stream_failure_message(event))
        if protocol == "responses":
            if event.get("type") == "response.output_text.delta":
                pieces.append(event.get("delta") or "")
            elif event.get("type") == "response.completed":
                result = event.get("response") or {}
                if result.get("status") not in (None, "completed") or result.get("error"):
                    raise ProbeError("上游响应未完整结束")
                if not pieces:
                    pieces.extend(part.get("text", "") for item in result.get("output", [])
                                  for part in item.get("content", []) if part.get("type") == "output_text")
                complete = True
                break
        elif protocol == "anthropic":
            if event.get("type") == "content_block_delta" and event.get("delta", {}).get("type") == "text_delta":
                pieces.append(event["delta"].get("text", ""))
            if event.get("type") == "message_delta" and event.get("delta", {}).get("stop_reason") == "max_tokens":
                raise ProbeError("检测回答被截断")
            if event.get("type") == "message_stop":
                complete = True
                break
        else:
            for choice in event.get("choices", []):
                pieces.append((choice.get("delta") or choice.get("message") or {}).get("content") or "")
                if choice.get("finish_reason") in ("length", "content_filter"):
                    raise ProbeError("检测回答被截断或过滤")
                if choice.get("finish_reason") == "stop":
                    complete = True
    if not complete or not pieces:
        raise ProbeError("流式响应中断或没有有效文本")
    return "".join(pieces)


def transport_failure_message(error, read_timeout):
    if isinstance(error, requests.ConnectTimeout):
        return "连接上游超时，本轮停止"
    # requests wraps urllib3's streaming read timeout in ConnectionError.
    # Inspect exception types, never echo URLs, credentials or raw error text.
    pending, seen = [error], set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, (requests.ReadTimeout, ReadTimeoutError)):
            return f"等待上游响应超时（连续 {read_timeout:g} 秒未收到数据），本轮停止"
        pending.extend(e for e in (current.__cause__, current.__context__, *current.args)
                       if isinstance(e, BaseException))
    if isinstance(error, requests.exceptions.SSLError):
        return "上游 TLS 连接失败，本轮停止"
    return "上游连接失败或连接中断，本轮停止"


def probe(account, model, challenge, cancelled, deadline, codex_version):
    if cancelled():
        raise Cancelled()
    url, headers, payload, protocol, actual = build_request(account, model, challenge["prompt"], codex_version)
    proxy = proxy_url(account)
    with requests.Session() as session:
        session.trust_env = False
        if proxy:
            session.proxies.update(http=proxy, https=proxy)
        try:
            # No retries, redirects, account fallback, or protocol fallback.
            remaining = deadline - time.monotonic()
            if remaining < 5:
                raise ProbeError("本轮检测超时")
            read_timeout = min(PROBE_READ_TIMEOUT, remaining)
            with session.post(url, headers=headers, json=payload, stream=True, allow_redirects=False,
                              timeout=(min(8, remaining), read_timeout)) as response:
                if response.status_code != 200:
                    raise ProbeError(f"上游 HTTP {response.status_code}，本轮停止")
                content_type = response.headers.get("Content-Type", "")
                # Codex may omit this header. Valid SSE completion is still required.
                if content_type and "text/event-stream" not in content_type:
                    raise ProbeError("上游没有返回预期的流式响应")
                output = read_stream(response, protocol, cancelled, deadline)
        except requests.RequestException as exc:
            if cancelled():
                raise Cancelled() from None
            if time.monotonic() >= deadline:
                raise ProbeError("本轮检测超时（240 秒预算已用尽）") from None
            raise ProbeError(transport_failure_message(exc, read_timeout)) from None
    minimum = max(80, math.ceil(challenge["expected_count"] * .55))
    parsed_count = len(parse_numbers(output))
    if parsed_count < minimum:
        raise InsufficientSample(f"有效数字样本不足：识别 {parsed_count} 个，至少需要 {minimum} 个"
                                 f"（本次要求 {challenge['expected_count']} 个），无法归因")
    return {"text": output, "expected_count": challenge["expected_count"]}, actual


def summarize(outputs, model):
    result = analyze_global_outputs(outputs, BANK)
    candidates = result["results"]
    probability = result["probability"]
    margin = probability - candidates[1]["probability"]
    confident = (1 <= result["used_outputs"] <= 3 and result["used_outputs"] == len(outputs)
                 and probability >= CONFIDENCE and margin >= MARGIN)
    status = "green" if confident and result["prediction"] == model else "yellow"
    return {"status": status, "prediction": result["prediction"], "probability": probability,
            "confident": confident, "samples": result["used_outputs"],
            "candidates": [{"model": c["model"], "probability": c["probability"]} for c in candidates[:3]],
            "message": "与预期一致" if status == "green" else "归因不一致，等待复测" if confident else "候选区分度不足，待确认"}


def run_round(job, store, accounts, codex_version, probe_fn=probe, *, cancelled=None, prompt_template=None):
    if cancelled is None:
        cancelled = lambda: not store.is_current(job)
    deadline = time.monotonic() + 240
    challenge_count = job.get("challenge_count", 3)
    request_limit = challenge_count * 2
    requests_sent, summaries = 0, []
    valid_samples, insufficient_samples = 0, 0
    last_insufficient = ''
    try:
        if prompt_template is not None:
            validate_prompt_template(prompt_template)
        account = accounts.get(job["account_id"])
        ensure_available(account)
        context = account_context(account)
        if job["model"] not in MODELS:
            raise ProbeError("指纹库未收录所选模型")
        # Keep successful responses within this round; replacements and confirmation
        # share twice the selected sample count. Never combine partial responses.
        challenges = iter(generate_challenges(request_limit))
        for attempt in range(2):
            outputs = []
            while len(outputs) < challenge_count and requests_sent < request_limit:
                if cancelled():
                    raise Cancelled()
                if deadline - time.monotonic() < 5:
                    raise ProbeError("本轮检测超时")
                account = accounts.get(job["account_id"])
                ensure_available(account)
                if account_context(account) != context:
                    raise ProbeError("账号路由或配置已变化，本轮作废")
                challenge = next(challenges)
                if prompt_template is not None:
                    challenge = {**challenge, "prompt": prompt_template.replace("{{challenge}}", challenge["prompt"])}
                requests_sent += 1
                try:
                    output, actual = probe_fn(account, job["model"], challenge, cancelled, deadline, codex_version)
                except InsufficientSample as exc:
                    insufficient_samples += 1
                    last_insufficient = str(exc)
                    continue
                outputs.append(output)
                valid_samples += 1
            if len(outputs) < challenge_count:
                stage = "初测" if attempt == 0 else "不符复测"
                raise ProbeError(f"{stage}仅取得 {len(outputs)}/{challenge_count} 份有效回答，已达本轮 {request_limit} 次请求上限，待确认。{last_insufficient}")
            summary = summarize(outputs, job["model"])
            summary["sent_model"] = actual
            summaries.append(summary)
            if attempt == 0 and summary["confident"] and summary["prediction"] != job["model"]:
                if request_limit - requests_sent < challenge_count:
                    summary.update(status="yellow", message=f"初测归因不符，但补采后剩余请求预算不足以完成 {challenge_count} 份独立复测，待确认")
                    break
                continue
            break
        result = summaries[-1]
        if len(summaries) == 2:
            first, second = summaries
            result["confirmation"] = [{"prediction": s["prediction"], "probability": s["probability"]} for s in summaries]
            if second["confident"] and first["prediction"] == second["prediction"] != job["model"]:
                result.update(status="red", message="复测仍与预期不符（统计归因，非定论）")
            else:
                result.update(status="yellow", message="两轮结果不稳定，待确认")
        if cancelled():
            raise Cancelled()
        latest = accounts.get(job["account_id"])
        ensure_available(latest)
        if account_context(latest) != context:
            raise ProbeError("账号路由或配置已变化，本轮作废")
        result["_context"] = context.hex()
    except ProbeError as exc:
        result = {"status": "yellow", "message": str(exc)}
    result.update(requests=requests_sent, valid_samples=valid_samples, insufficient_samples=insufficient_samples,
                  challenge_count=challenge_count, request_limit=request_limit)
    return result


def run_prompt_round(job, store, accounts, controller):
    cancelled = lambda: not store.is_prompt_current(job)
    started = time.monotonic()
    if cancelled():
        raise Cancelled()
    try:
        validate_prompt_template(job["prompt"])
        controller.check_prompt_support()
    except ProbeError as exc:
        return {"status": "yellow", "message": str(exc), "requests": 0,
                "valid_samples": 0, "insufficient_samples": 0,
                "challenge_count": job.get("challenge_count", 3),
                "request_limit": job.get("challenge_count", 3) * 2,
                "latency_ms": int((time.monotonic() - started) * 1000)}

    def gateway_probe(account, model, challenge, cancelled, deadline, _version):
        output = controller.prompt(account["id"], model, challenge["prompt"], cancelled, deadline)
        minimum = max(80, math.ceil(challenge["expected_count"] * .55))
        count = len(parse_numbers(output))
        if count < minimum:
            raise InsufficientSample(f"有效数字样本不足：识别 {count} 个，至少需要 {minimum} 个"
                                     f"（本次要求 {challenge['expected_count']} 个），无法归因")
        return {"text": output, "expected_count": challenge["expected_count"]}, mapped_model(model, account["credentials"].get("model_mapping"))

    result = run_round(job, store, accounts, "", gateway_probe,
                       cancelled=cancelled, prompt_template=job["prompt"])
    result["latency_ms"] = int((time.monotonic() - started) * 1000)
    return result


def scheduler(store, accounts, stop, codex_version, controller):
    store.recover()
    prefer_prompt = False
    while not stop.wait(2):
        try:
            store.apply_pending_pause(accounts, controller)
            store.apply_pending_prompt_pause(accounts, controller)
            if hasattr(os, "getloadavg") and os.getloadavg()[0] > max(1, (os.cpu_count() or 1) * .8):
                continue
            job = None
            prompt_job = None
            if prefer_prompt:
                prompt_job = store.claim_prompt()
                if prompt_job is None:
                    job = store.claim()
            else:
                job = store.claim()
                if job is None:
                    prompt_job = store.claim_prompt()
            prefer_prompt = not prefer_prompt
            if job is not None:
                try:
                    result = run_round(job, store, accounts, codex_version)
                except Cancelled:
                    continue
                except Exception as exc:
                    LOG.warning("probe failed account_id=%s exception=%s", job["account_id"], type(exc).__name__)
                    result = {"status": "yellow", "message": "检测暂不可用，等待下一轮"}
                store.finish(job, result)
            else:
                if prompt_job is None:
                    continue
                try:
                    prompt_result = run_prompt_round(prompt_job, store, accounts, controller)
                except Cancelled:
                    continue
                except Exception as exc:
                    LOG.warning("prompt test failed account_id=%s exception=%s", prompt_job["account_id"], type(exc).__name__)
                    prompt_result = {"status": "yellow", "message": "提示词测试暂不可用，等待下一轮"}
                store.finish_prompt(prompt_job, prompt_result)
            store.apply_pending_pause(accounts, controller)
            store.apply_pending_prompt_pause(accounts, controller)
        except Exception as exc:
            LOG.warning("scheduler exception=%s", type(exc).__name__)
            stop.wait(15)


class SchedulingControl:
    """Use native testing and pause endpoints; the database role remains read-only."""
    def __init__(self, key_path, base_url="http://127.0.0.1:8080"):
        self.key_path = Path(key_path)
        self.base_url = base_url.rstrip("/")

    def _key(self):
        key = self.key_path.read_text().strip()
        if not key or len(key) > 16384:
            raise ProbeError("停止调度凭据不可用")
        return key

    def pause(self, account_id):
        if type(account_id) is not int or account_id <= 0:
            raise ProbeError("账号 ID 无效")
        key = self._key()
        with requests.Session() as session:
            session.trust_env = False
            response = session.post(
                f"{self.base_url}/api/v1/admin/accounts/{account_id}/schedulable",
                headers={"X-API-Key": key}, json={"schedulable": False},
                timeout=(1, 3), allow_redirects=False)
            if response.status_code != 200:
                raise ProbeError("停止调度接口未成功")
            payload = response.json()
            data = payload.get("data") or {}
            if payload.get("code") != 0 or data.get("id") != account_id or data.get("schedulable") is not False:
                raise ProbeError("停止调度未获确认")

    def check_prompt_support(self):
        try:
            with requests.Session() as session:
                session.trust_env = False
                response = session.get(self.base_url + "/api/v1/admin/accounts/test-capabilities",
                                       headers={"X-API-Key": self._key()}, timeout=(1, 3), allow_redirects=False)
                if response.status_code == 200:
                    payload = response.json()
                    data = payload.get("data") or {}
                    if payload.get("code") == 0 and data.get("attribution_prompt_v1") is True:
                        return
        except (requests.RequestException, ValueError, AttributeError):
            pass
        raise ProbeError("GN2API 尚未支持自定义归因测试接口，本轮未发送模型请求；请先安装接口修复")

    def prompt(self, account_id, model, prompt, cancelled=lambda: False, deadline=None):
        if type(account_id) is not int or account_id <= 0:
            raise ProbeError("账号 ID 无效")
        if not isinstance(model, str) or not model or len(model) > 160:
            raise ProbeError("测试模型无效")
        if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > PROMPT_MAX_CHARS + 2000:
            raise ProbeError("测试提示词无效")
        key = self._key()
        pieces, total, complete = [], 0, False
        deadline = deadline if deadline is not None else time.monotonic() + 240
        remaining = deadline - time.monotonic()
        if cancelled():
            raise Cancelled()
        if remaining < 5:
            raise ProbeError("本轮检测超时")
        try:
            with requests.Session() as session:
                session.trust_env = False
                with session.post(
                    f"{self.base_url}/api/v1/admin/accounts/{account_id}/test",
                    headers={"X-API-Key": key, "Accept": "text/event-stream"},
                    json={"model_id": model, "prompt": prompt, "mode": "attribution"},
                    stream=True, allow_redirects=False, timeout=(min(8, remaining), min(PROBE_READ_TIMEOUT, remaining)),
                ) as response:
                    if response.status_code != 200:
                        raise ProbeError("账号测试接口返回错误")
                    for raw in response.iter_lines(chunk_size=256):
                        if cancelled():
                            raise Cancelled()
                        if time.monotonic() >= deadline:
                            raise ProbeError("本轮检测超时")
                        total += len(raw)
                        if total > MAX_RESPONSE_BYTES:
                            raise ProbeError("测试输出超出限制")
                        if not raw.startswith(b"data:"):
                            continue
                        try:
                            event = json.loads(raw[5:].strip())
                        except (ValueError, UnicodeDecodeError):
                            raise ProbeError("账号测试返回了无法解析的流式数据") from None
                        if not isinstance(event, dict):
                            raise ProbeError("账号测试返回了无效的流式数据")
                        event_type = event.get("type")
                        if event_type == "content":
                            text = event.get("text")
                            if isinstance(text, str):
                                pieces.append(text)
                        elif event_type == "error":
                            raise ProbeError("账号测试失败")
                        elif event_type == "test_complete":
                            complete = event.get("success") is True
                            if not complete:
                                raise ProbeError("账号测试未完成")
                            break
        except Cancelled:
            raise
        except ProbeError:
            raise
        except requests.RequestException as exc:
            raise ProbeError(transport_failure_message(exc, min(PROBE_READ_TIMEOUT, remaining))) from None
        if not complete or not "".join(pieces).strip():
            raise ProbeError("账号测试没有返回完整回答")
        return "".join(pieces)


class AdminAuth:
    def __init__(self, base_url):
        self.base = base_url.rstrip("/")
        self.cache = {}
        self.lock = threading.Lock()

    def __call__(self, headers):
        forwarded = {k: headers[k] for k in ("Authorization", "X-API-Key") if headers.get(k)}
        if not forwarded or any(len(v) > 16384 for v in forwarded.values()):
            return False
        key = hashlib.sha256(json.dumps(forwarded, sort_keys=True).encode()).digest()
        with self.lock:
            if self.cache.get(key, 0) > time.time():
                return True
        try:
            with requests.Session() as session:
                session.trust_env = False
                r = session.get(self.base + "/api/v1/admin/system/version", headers=forwarded,
                                timeout=5, allow_redirects=False)
                valid = r.status_code == 200 and r.headers.get("Content-Type", "").startswith("application/json") and r.json().get("code") == 0
        except (requests.RequestException, ValueError):
            return False
        if valid:
            with self.lock:
                if len(self.cache) > 100:
                    self.cache.clear()
                self.cache[key] = time.time() + 10
        return valid


def create_app(store, accounts, authenticate):
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 16 * 1024
    allowed_origins = set(os.environ.get("MODELTRACE_ORIGINS", "https://aifxo.com,https://www.aifxo.com").split(","))

    @app.before_request
    def guard():
        if request.path == "/healthz":
            return None
        if request.method not in ("GET", "PUT"):
            return jsonify(error="不支持此操作"), 405
        if request.headers.get("Origin") and request.headers["Origin"] not in allowed_origins:
            return jsonify(error="来源不允许"), 403
        if not authenticate(request.headers):
            return jsonify(error="请使用管理员账号登录"), 401

    @app.after_request
    def no_cache(response):
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.errorhandler(413)
    def too_large(_):
        return jsonify(error="请求过大"), 413

    @app.errorhandler(Exception)
    def failed(exc):
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            return jsonify(error="请求无效"), exc.code
        LOG.warning("api exception=%s", type(exc).__name__)
        return jsonify(error="检测服务暂不可用"), 503

    @app.get("/healthz")
    def health():
        return jsonify(status="ok", bank=BANK_COMMIT)

    def render(account_id, meta):
        result = store.public(account_id)
        result.update(available=not unavailable(meta), reason=unavailable(meta), models=model_choices(meta),
                      mapping=meta.get("mapping") or {}, platform=meta["platform"],
                      schedulable=bool(meta.get("schedulable")))
        result["prompt_test"]["available"] = not prompt_unavailable(meta)
        result["prompt_test"]["reason"] = prompt_unavailable(meta)
        return result

    @app.get("/accounts")
    def states():
        ids = request.args.get("ids", "")
        if not re.fullmatch(r"[0-9,]{1,2000}", ids):
            return jsonify(error="账号 ID 无效"), 400
        ids = list(dict.fromkeys(int(x) for x in ids.split(",") if x))
        if not ids or len(ids) > 100 or any(x <= 0 or x > 2**63-1 for x in ids):
            return jsonify(error="最多查询 100 个账号"), 400
        meta = accounts.metadata(ids)
        return jsonify(accounts={str(i): render(i, m) for i, m in meta.items()},
                       interval=INTERVAL, max_history=MAX_HISTORY, bank=BANK_COMMIT)

    @app.put("/accounts/<int:account_id>")
    def configure(account_id):
        body = request.get_json(silent=True)
        if not isinstance(body, dict) or type(body.get("enabled")) is not bool:
            return jsonify(error="开关参数无效"), 400
        meta = accounts.metadata([account_id]).get(account_id)
        if not meta:
            return jsonify(error="账号不存在"), 404
        previous = store.get(account_id)
        model = body.get("model", previous["model"] if previous else "")
        if not isinstance(model, str) or len(model) > 160:
            return jsonify(error="模型无效"), 400
        auto_pause = body.get("auto_pause_enabled", bool(previous and previous["auto_pause_enabled"]))
        threshold = body.get("auto_pause_threshold", previous["auto_pause_threshold"] if previous else 3)
        interval_minutes = body.get("interval_minutes", previous["interval_minutes"] if previous else 10)
        challenge_count = body.get("challenge_count", previous["challenge_count"] if previous else 3)
        if type(challenge_count) is not int or challenge_count not in (1, 2, 3):
            return jsonify(error="挑战次数请选择 1、2 或 3 次"), 400
        if type(interval_minutes) is not int or not 1 <= interval_minutes <= 1440:
            return jsonify(error="检测间隔请填写 1–1440 分钟的整数"), 400
        if type(auto_pause) is not bool or type(threshold) is not int or not 1 <= threshold <= 100:
            return jsonify(error="自动停调度开关无效，或连续轮数不在 1–100 之间"), 400
        if body["enabled"]:
            # Allow cancelling automatic action even if the account became unavailable.
            cancelling_only = (previous and previous["enabled"] and previous["model"] == model
                               and not auto_pause and threshold == previous["auto_pause_threshold"])
            if unavailable(meta) and not cancelling_only:
                return jsonify(error=unavailable(meta)), 400
            if model not in model_choices(meta) and not cancelling_only:
                return jsonify(error="请选择指纹库已收录且账号支持的模型"), 400
        store.configure(account_id, body["enabled"], model, auto_pause, threshold, interval_minutes, challenge_count)
        return jsonify(account=render(account_id, meta))

    @app.put("/accounts/<int:account_id>/prompt-test")
    def configure_prompt(account_id):
        body = request.get_json(silent=True)
        if not isinstance(body, dict) or type(body.get("enabled")) is not bool:
            return jsonify(error="提示词测试开关参数无效"), 400
        meta = accounts.metadata([account_id]).get(account_id)
        if not meta:
            return jsonify(error="账号不存在"), 404
        previous = store.get_prompt(account_id)
        model = body.get("model", previous["model"] if previous else "gpt-6-astra")
        prompt = body.get("prompt", previous["prompt"] if previous else DEFAULT_PROMPT)
        auto_pause = body.get("auto_pause_enabled", bool(previous and previous["auto_pause_enabled"]))
        threshold = body.get("auto_pause_threshold", previous["auto_pause_threshold"] if previous else 3)
        interval_minutes = body.get("interval_minutes", previous["interval_minutes"] if previous else 10)
        challenge_count = body.get("challenge_count", previous["challenge_count"] if previous else 3)
        if not isinstance(model, str) or not model.strip() or len(model) > 160:
            return jsonify(error="测试模型无效"), 400
        if body["enabled"]:
            try:
                validate_prompt_template(prompt)
            except ProbeError as exc:
                return jsonify(error=str(exc)), 400
        elif previous:
            prompt = previous["prompt"]
        if type(challenge_count) is not int or challenge_count not in (1, 2, 3):
            return jsonify(error="挑战次数请选择 1、2 或 3 次"), 400
        if type(auto_pause) is not bool or type(threshold) is not int or not 1 <= threshold <= 100:
            return jsonify(error="自动停调度开关无效，或连续红点轮数不在 1–100 之间"), 400
        if type(interval_minutes) is not int or not 1 <= interval_minutes <= 1440:
            return jsonify(error="测试间隔请填写 1–1440 分钟的整数"), 400
        if body["enabled"]:
            if prompt_unavailable(meta):
                return jsonify(error=prompt_unavailable(meta)), 400
            if model.strip() not in model_choices(meta):
                return jsonify(error="请选择指纹库已收录且账号支持的模型"), 400
        store.configure_prompt(account_id, body["enabled"], model.strip(), prompt,
                               auto_pause, threshold, interval_minutes, challenge_count)
        return jsonify(account=render(account_id, meta))

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(name)s %(levelname)s %(message)s")
    store = Store(os.environ.get("MODELTRACE_DB", "/var/lib/modeltrace-ovh/state.sqlite3"))
    accounts = Accounts(os.environ.get("MODELTRACE_PG", "dbname=sub2api user=modeltrace_ovh host=/var/run/postgresql"))
    auth = AdminAuth(os.environ.get("SUB2API_URL", "http://127.0.0.1:8080"))
    app = create_app(store, accounts, auth)
    stop = threading.Event()
    controller = SchedulingControl(
        Path(os.environ.get("CREDENTIALS_DIRECTORY", "/etc/modeltrace-ovh")) / "sub2api-admin-key",
        os.environ.get("SUB2API_URL", "http://127.0.0.1:8080"),
    )
    worker = threading.Thread(target=scheduler, args=(store, accounts, stop,
        os.environ.get("CODEX_CLIENT_VERSION", "0.147.0"), controller), daemon=True)
    worker.start()
    serve(app, host="127.0.0.1", port=int(os.environ.get("MODELTRACE_PORT", "17861")),
          threads=4, channel_timeout=20, ident="", expose_tracebacks=False)
