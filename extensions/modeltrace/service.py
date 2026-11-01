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
import unicodedata
import uuid

import attempts
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

COMPARISON_PROFILE = "codex-http-xhigh-v1"
REASONING_EFFORTS = ("low", "medium", "high", "xhigh")
ATTRIBUTION_INITIAL_SECONDS = 720
ATTRIBUTION_MAX_SECONDS = 1200
COMPARISON_INSTRUCTIONS = "Follow the user's instructions exactly. Do not use tools."
BANK_COMMIT = "6cd4a8ff0f3f4e49d6d6969dd2fbadfa1569fde2"
BANK = load_bank(ROOT / "vendor/data/unified_bank.json")
MODELS = {m["id"]: m for m in BANK["models"]}
INTERVAL = 600
MAX_HISTORY = 10
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
PROMPT_MAX_CHARS = 4000
DEFAULT_PROMPT = "{{challenge}}"
QA_MAX_CASES = 5
MAX_ARTIFACT_BYTES = 512 * 1024
DEFAULT_QA_CASES = [
    {"id": "candy", "name": "糖果形状题", "enabled": False,
     "prompt": "黑色袋子中有苹果味、桃子味、西瓜味三种糖果。圆形分别有 7、9、8 颗，五角星形分别有 7、6、4 颗。"
               "可以靠手感辨别并选择形状，但不能靠手感辨别口味。活动前决定取出的总数，最少取多少颗，"
               "才能保证同时有圆形苹果味和五角星桃子味，或者圆形桃子味和五角星苹果味？只回答最少颗数的阿拉伯数字，不要解释。",
     "expected_answer": "21"},
    {"id": "pelican_bicycle", "name": "鹈鹕骑自行车动画", "enabled": False, "kind": "manual",
     "prompt": "创建一个HTML，内容是SVG绘制一个鹈鹕骑自行车的2D动画，你不需要任何测试，不需要调用任何skills",
     "expected_answer": ""},
    {"id": "tibo", "name": "Tibo 题", "enabled": False, "kind": "exact",
     "prompt": "Tell me whether you know who Tibo is in OpenAI based on your knowledge without searching online. Only return True or False.",
     "expected_answer": "True"},
]
PROBE_READ_TIMEOUT = 120
CONFIDENCE = 0.80  # Conservative display threshold, not a guarantee of identity.
MARGIN = 0.25
LOG = logging.getLogger("modeltrace")


class ProbeError(Exception):
    """Only sanitized, user-readable messages may enter this exception."""
    def __init__(self, message, code="test_error"):
        super().__init__(message)
        self.code = code


class RoundTimeout(ProbeError):
    def __init__(self):
        super().__init__("本轮检测超时（整轮时间预算已用尽）", "round_timeout")


class InsufficientSample(ProbeError):
    """A complete response with too few numbers; a new challenge may replace it."""
    def __init__(self, message):
        super().__init__(message, "insufficient_samples")


class Cancelled(Exception):
    pass


def comparison_profile(effort="xhigh"):
    if effort not in REASONING_EFFORTS:
        raise ProbeError("推理等级请选择 low、medium、high 或 xhigh", "invalid_effort")
    return f"codex-http-{effort}-v1"


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
                CREATE TABLE IF NOT EXISTS prompt_artifacts (
                    result_id INTEGER NOT NULL, case_id TEXT NOT NULL, html TEXT NOT NULL, prompt TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(result_id, case_id)
                );
            """)
            attempts.initialize(db)
            # Migrate existing installations without losing settings or history.
            columns = {row[1] for row in db.execute("PRAGMA table_info(monitors)")}
            for name, definition in {
                "reasoning_effort": "TEXT NOT NULL DEFAULT 'xhigh'",
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
            prompt_columns = {row[1] for row in db.execute("PRAGMA table_info(prompt_monitors)")}
            for name, definition in {
                "reasoning_effort": "TEXT NOT NULL DEFAULT 'xhigh'",
                "test_mode": "TEXT NOT NULL DEFAULT 'attribution'",
                "cases": "TEXT NOT NULL DEFAULT '[]'",
            }.items():
                if name not in prompt_columns:
                    db.execute(f"ALTER TABLE prompt_monitors ADD COLUMN {name} {definition}")

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
                         challenge_count=None, test_mode=None, cases=None, reasoning_effort=None):
        with self.action_lock, self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM prompt_monitors WHERE account_id=?", (account_id,)).fetchone()
            reasoning_effort = (old["reasoning_effort"] if old else "xhigh") if reasoning_effort is None else reasoning_effort
            comparison_profile(reasoning_effort)
            auto_pause_enabled = bool(old and old["auto_pause_enabled"]) if auto_pause_enabled is None else auto_pause_enabled
            auto_pause_threshold = (old["auto_pause_threshold"] if old else 3) if auto_pause_threshold is None else auto_pause_threshold
            interval_minutes = (old["interval_minutes"] if old else 10) if interval_minutes is None else interval_minutes
            challenge_count = (old["challenge_count"] if old else 3) if challenge_count is None else challenge_count
            test_mode = (old["test_mode"] if old else "attribution") if test_mode is None else test_mode
            cases_json = (old["cases"] if old else "[]") if cases is None else json.dumps(cases, ensure_ascii=False, sort_keys=True)
            same = (old and bool(old["enabled"]) == enabled and old["model"] == model
                    and old["reasoning_effort"] == reasoning_effort
                    and old["prompt"] == prompt and old["challenge_count"] == challenge_count
                    and old["test_mode"] == test_mode and old["cases"] == cases_json
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
                    auto_pause_enabled,auto_pause_threshold,interval_minutes,challenge_count,test_mode,cases,reasoning_effort)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id) DO UPDATE SET
                enabled=excluded.enabled, model=excluded.model, prompt=excluded.prompt,
                test_mode=excluded.test_mode, cases=excluded.cases,
                reasoning_effort=excluded.reasoning_effort,
                challenge_count=excluded.challenge_count, generation=generation+1,
                next_run=excluded.next_run, updated_at=excluded.updated_at,
                auto_pause_enabled=excluded.auto_pause_enabled,
                auto_pause_threshold=excluded.auto_pause_threshold,
                interval_minutes=excluded.interval_minutes,
                failure_streak=0,failure_context='',pause_status='',pause_retry_at=0,
                paused_at=0,pause_message='',running=0,message=''""",
                (account_id, int(enabled), model, prompt, next_run, time.time(),
                 int(auto_pause_enabled), auto_pause_threshold, interval_minutes, challenge_count, test_mode, cases_json, reasoning_effort))

    def configure(self, account_id, enabled, model, auto_pause_enabled=None, auto_pause_threshold=None,
                  interval_minutes=None, challenge_count=None, reasoning_effort=None):
        with self.action_lock, self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM monitors WHERE account_id=?", (account_id,)).fetchone()
            reasoning_effort = (old["reasoning_effort"] if old else "xhigh") if reasoning_effort is None else reasoning_effort
            comparison_profile(reasoning_effort)
            auto_pause_enabled = bool(old and old["auto_pause_enabled"]) if auto_pause_enabled is None else auto_pause_enabled
            auto_pause_threshold = (old["auto_pause_threshold"] if old else 3) if auto_pause_threshold is None else auto_pause_threshold
            interval_minutes = (old["interval_minutes"] if old else 10) if interval_minutes is None else interval_minutes
            challenge_count = (old["challenge_count"] if old else 3) if challenge_count is None else challenge_count
            # Repeated identical requests must not schedule extra paid probes.
            if (old and bool(old["enabled"]) == enabled and old["model"] == model
                    and old["reasoning_effort"] == reasoning_effort
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
                    auto_pause_enabled,auto_pause_threshold,interval_minutes,challenge_count,reasoning_effort)
                VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id) DO UPDATE SET
                enabled=excluded.enabled, model=excluded.model, generation=generation+1,
                next_run=excluded.next_run, updated_at=excluded.updated_at,
                auto_pause_enabled=excluded.auto_pause_enabled,
                auto_pause_threshold=excluded.auto_pause_threshold,
                interval_minutes=excluded.interval_minutes,
                challenge_count=excluded.challenge_count,
                reasoning_effort=excluded.reasoning_effort,
                mismatch_streak=0,mismatch_context='',pause_status='',pause_retry_at=0,
                running=0, message=''""",
                (account_id, int(enabled), model, next_run, time.time(),
                 int(auto_pause_enabled), auto_pause_threshold, interval_minutes, challenge_count, reasoning_effort))

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
            if (current["auto_pause_enabled"] and result.get("status") == "red" and context
                    and result.get("bank_applicability") == "validated"):
                streak = (current["mismatch_streak"] if current["mismatch_context"] == context else 0) + 1
            pending = streak >= current["auto_pause_threshold"]
            result.update(checked_at=now, expected=job["model"], bank_commit=BANK_COMMIT)
            result["reasoning_effort"] = job.get("reasoning_effort", "xhigh")
            result["mismatch_streak"] = streak
            db.execute("INSERT INTO results(account_id,checked_at,model,result) VALUES(?,?,?,?)",
                       (job["account_id"], now, job["model"], json.dumps(result, ensure_ascii=False)))
            db.execute("""UPDATE monitors SET running=0,message=?,mismatch_streak=?,mismatch_context=?,
                        pause_status=?,pause_retry_at=0,
                        next_run=CASE WHEN next_run<=? THEN ? ELSE next_run END WHERE account_id=?""",
                       (result.get("message", ""), streak, context if streak else "",
                        "pending" if pending else "", now, now + current["interval_minutes"] * 60, job["account_id"]))

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
            artifacts = result.pop("_artifacts", {})
            manual_round = job.get("test_mode") == "qa" and any(
                case.get("enabled") and case.get("kind") == "manual" for case in json.loads(job["cases"]))
            streak = 0
            if (current["auto_pause_enabled"] and result.get("status") == "red" and context and not manual_round
                    and (job.get("test_mode") == "qa" or result.get("bank_applicability") == "validated")):
                streak = (current["failure_streak"] if current["failure_context"] == context else 0) + 1
            pending = streak >= current["auto_pause_threshold"]
            mode = job.get("test_mode", "attribution")
            result.update(checked_at=now, expected=job["model"], test_mode=mode,
                          reasoning_effort=job.get("reasoning_effort", "xhigh"),
                          template_hash=hashlib.sha256((job["cases"] if mode == "qa" else job["prompt"]).encode()).hexdigest())
            if mode == "attribution":
                result["bank_commit"] = BANK_COMMIT
            result["failure_streak"] = streak
            inserted = db.execute("INSERT INTO prompt_results(account_id,checked_at,model,result) VALUES(?,?,?,?)",
                                  (job["account_id"], now, job["model"], json.dumps(result, ensure_ascii=False)))
            for case_id, artifact in artifacts.items():
                db.execute("INSERT INTO prompt_artifacts(result_id,case_id,html,prompt) VALUES(?,?,?,?)",
                           (inserted.lastrowid, case_id, artifact["html"], artifact["prompt"]))
            db.execute("""UPDATE prompt_monitors SET running=0,message=?,failure_streak=?,failure_context=?,
                        pause_status=?,pause_retry_at=0,
                        next_run=CASE WHEN next_run<=? THEN ? ELSE next_run END WHERE account_id=?""",
                       (result.get("message", ""), streak, context if streak else "",
                        "pending" if pending else "", now, now + current["interval_minutes"] * 60, job["account_id"]))

    def artifact(self, account_id, result_id, case_id):
        with self.db() as db:
            row = db.execute("""SELECT a.html,a.prompt FROM prompt_artifacts a
                JOIN prompt_results r ON r.id=a.result_id
                WHERE r.account_id=? AND a.result_id=? AND a.case_id=?""",
                (account_id, result_id, case_id)).fetchone()
            return dict(row) if row else None

    def review_artifact(self, account_id, result_id, case_id, status):
        # Human review only edits this result. It never changes scheduling or red streaks.
        with self.action_lock, self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("""SELECT r.result FROM prompt_results r
                JOIN prompt_artifacts a ON a.result_id=r.id
                WHERE r.account_id=? AND r.id=? AND a.case_id=?""",
                (account_id, result_id, case_id)).fetchone()
            if not row:
                return False
            result = json.loads(row[0])
            case = next((item for item in result.get("cases", []) if item["id"] == case_id and item.get("kind") == "manual"), None)
            if not case:
                return False
            reviewed_at = time.time()
            case.update(status=status, review_status="reviewed", reviewed_at=reviewed_at, review_source="admin_manual",
                        message={"green": "人工确认：符合要求", "yellow": "人工标记：待确认", "red": "人工确认：不符合要求"}[status])
            case["reviews"] = (case.get("reviews", []) + [{"status": status, "at": reviewed_at}])[-10:]
            if result.get("round_complete"):
                result["status"] = qa_status(result["cases"])
                result["message"] = {"green": "所选题目均通过（含人工评分）", "yellow": "含待确认结果（人工评分已保存）",
                                     "red": "有题目未通过（含人工评分）"}[result["status"]]
            result["manual_reviewed"] = True
            db.execute("UPDATE prompt_results SET result=? WHERE id=? AND account_id=?",
                       (json.dumps(result, ensure_ascii=False), result_id, account_id))
            return True

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
            latest = db.execute("SELECT result FROM results WHERE account_id=? ORDER BY id DESC LIMIT 1",
                                (row["account_id"],)).fetchone()
            if not latest or json.loads(latest[0]).get("bank_applicability") != "validated":
                db.execute("""UPDATE monitors SET mismatch_streak=0,mismatch_context='',pause_status='',
                    pause_retry_at=0,message='指纹库适用性未验证，已取消自动停调度' WHERE account_id=?""",
                           (row["account_id"],))
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
            latest = db.execute("SELECT result FROM prompt_results WHERE account_id=? ORDER BY id DESC LIMIT 1",
                                (row["account_id"],)).fetchone()
            if row["test_mode"] != "qa" and (not latest or json.loads(latest[0]).get("bank_applicability") != "validated"):
                db.execute("""UPDATE prompt_monitors SET failure_streak=0,failure_context='',pause_status='',
                    pause_retry_at=0,message='指纹库适用性未验证，已取消自动停调度' WHERE account_id=?""",
                           (row["account_id"],))
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
            kind = "问答测试复测仍未通过" if row["test_mode"] == "qa" else "提示词归因复测仍不符"
            message = f"连续 {row['failure_streak']} 轮{kind}，已停止此账号调度；恢复需手动操作"
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
            prompt_rows = db.execute("SELECT id,result FROM prompt_results WHERE account_id=? ORDER BY id DESC LIMIT ?",
                                     (account_id, MAX_HISTORY)).fetchall()
        prompt_public = {
            "enabled": bool(prompt and prompt["enabled"]),
            "model": prompt["model"] if prompt else "",
            "reasoning_effort": prompt["reasoning_effort"] if prompt else "xhigh",
            "reasoning_efforts": list(REASONING_EFFORTS),
            "prompt": prompt["prompt"] if prompt else DEFAULT_PROMPT,
            "test_mode": prompt["test_mode"] if prompt else "qa",
            "cases": json.loads(prompt["cases"]) if prompt else DEFAULT_QA_CASES,
            "qa_presets": DEFAULT_QA_CASES,
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
            "history": [{**json.loads(r[1]), "result_id": r[0]} for r in reversed(prompt_rows)],
        }
        return {
            "account_id": account_id, "enabled": bool(config and config["enabled"]),
            "model": config["model"] if config else "", "running": bool(config and config["running"]),
            "reasoning_effort": config["reasoning_effort"] if config else "xhigh",
            "reasoning_efforts": list(REASONING_EFFORTS),
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


def unavailable(meta, *, require_bank=True):
    if meta["platform"] != "openai" or meta["type"] != "oauth":
        return "当前固定条件比较仅支持 OpenAI OAuth；此账号未发送比较请求"
    if meta["platform"] not in ("openai", "anthropic"):
        return "指纹库暂未支持此平台"
    if meta["type"] not in ("apikey", "oauth") or (meta["platform"] == "anthropic" and meta["type"] != "apikey"):
        return "暂未适配此账号类型"
    if meta.get("parent_account_id") or meta.get("auth_mode") == "agent_identity":
        return "此特殊认证账号暂未适配"
    if meta["status"] != "active" or not meta.get("schedulable"):
        return "账号当前未启用或不可调度"
    if require_bank and not model_choices(meta):
        return "账号模型尚未被指纹库收录"
    return ""


def prompt_unavailable(meta, test_mode="attribution"):
    return unavailable(meta, require_bank=test_mode != "qa")


def validate_qa_cases(cases, enabled):
    if not isinstance(cases, list) or len(cases) > QA_MAX_CASES:
        raise ProbeError(f"最多保存 {QA_MAX_CASES} 道问答题")
    normalized, ids = [], set()
    for case in cases:
        if not isinstance(case, dict) or type(case.get("enabled")) is not bool:
            raise ProbeError("每道题的启用开关无效")
        identity = case.get("id")
        if not isinstance(identity, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,48}", identity) or identity in ids:
            raise ProbeError("题目编号无效或重复")
        ids.add(identity)
        kind = case.get("kind", "exact")
        if kind not in ("exact", "manual"):
            raise ProbeError("题目判断方式无效")
        cleaned = {"id": identity, "enabled": case["enabled"], "kind": kind}
        for field, maximum, label in (("name", 80, "题目名称"), ("prompt", PROMPT_MAX_CHARS, "提示词"),
                                      ("expected_answer", 200, "预期答案")):
            value = case.get(field, "")
            if not isinstance(value, str) or len(value) > maximum:
                raise ProbeError(f"{label}不能超过 {maximum} 个字符")
            if case["enabled"] and not value.strip() and not (field == "expected_answer" and kind == "manual"):
                raise ProbeError(f"启用的题目必须填写{label}")
            cleaned[field] = value
        normalized.append(cleaned)
    if enabled and not any(case["enabled"] for case in normalized):
        raise ProbeError("请至少勾选一道题，或关闭定时测试后保存")
    return normalized


def normalized_answer(text):
    return unicodedata.normalize("NFKC", text).strip().casefold()


def qa_status(cases):
    colors = {item["status"] for item in cases}
    return "yellow" if not cases or "yellow" in colors else "red" if "red" in colors else "green"


def html_artifact(output):
    # Keep the model's document as data; only unwrap a single complete Markdown fence.
    text = output.strip()
    fenced = re.fullmatch(r"```(?:html|svg|xml)?\s*\n(.*?)\n```", text, flags=re.S | re.I)
    if fenced:
        text = fenced.group(1).strip()
    if not text:
        raise ProbeError("模型未返回可预览的内容")
    if len(text.encode("utf-8")) > MAX_ARTIFACT_BYTES:
        raise ProbeError("生成内容超过 512 KiB 预览上限，本题待确认")
    return text


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


def ensure_available(account, *, require_bank=True):
    c = account["credentials"]
    reason = unavailable({**account, "mapping": c.get("model_mapping"), "auth_mode": c.get("auth_mode")},
                         require_bank=require_bank)
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
             account.get("proxy_id"), account.get("proxy_host"), account.get("proxy_port"),
             account.get("proxy_protocol"), account.get("proxy_username"), account.get("proxy_password")]
    # Quota snapshots may legitimately change while probes run.
    parts[9] = {key: (account.get("extra") or {}).get(key) for key in
                ("openai_responses_mode", "openai_responses_supported", "codex_timezone", "codex_fingerprint_seed", "codex_fingerprint_mode")}
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
        attempts.stream_line(raw, limit=MAX_RESPONSE_BYTES)
        if cancelled():
            raise Cancelled()
        if time.monotonic() > deadline:
            raise RoundTimeout()
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
    attempts.request_snapshot(url, headers, payload, protocol)
    with requests.Session() as session:
        session.trust_env = False
        if proxy:
            session.proxies.update(http=proxy, https=proxy)
        try:
            # No retries, redirects, account fallback, or protocol fallback.
            remaining = deadline - time.monotonic()
            if remaining < 5:
                raise RoundTimeout()
            read_timeout = min(PROBE_READ_TIMEOUT, remaining)
            with session.post(url, headers=headers, json=payload, stream=True, allow_redirects=False,
                              timeout=(min(8, remaining), read_timeout)) as response:
                attempts.response_snapshot(response)
                if response.status_code != 200:
                    attempts.error_body(response, MAX_RESPONSE_BYTES)
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
                raise RoundTimeout() from None
            raise ProbeError(transport_failure_message(exc, read_timeout)) from None
    minimum = max(80, math.ceil(challenge["expected_count"] * .55))
    parsed_count = len(parse_numbers(output))
    attempts.emit("parsed_output", {"output": output, "parsed_count": parsed_count, "minimum_count": minimum})
    if parsed_count < minimum:
        raise InsufficientSample(f"有效数字样本不足：识别 {parsed_count} 个，至少需要 {minimum} 个"
                                 f"（本次要求 {challenge['expected_count']} 个），无法归因")
    return {"text": output, "expected_count": challenge["expected_count"]}, actual


def summarize(outputs, model):
    result = analyze_global_outputs(outputs, BANK)
    candidates = result["results"]
    probability = result["probability"]
    margin = probability - candidates[1]["probability"]
    separated = (1 <= result["used_outputs"] <= 3 and result["used_outputs"] == len(outputs)
                 and probability >= CONFIDENCE and margin >= MARGIN)
    confident = False
    status = "yellow"
    return {"status": status, "prediction": result["prediction"], "probability": probability,
            "confident": confident, "samples": result["used_outputs"],
            "analysis": result, "candidate_separated": separated, "bank_applicability": "unverified",
            "comparison_profile": COMPARISON_PROFILE,
            "bank_sha256": hashlib.sha256((ROOT / "vendor/data/unified_bank.json").read_bytes()).hexdigest(),
            "candidates": [{"model": c["model"], "probability": c["probability"]} for c in candidates[:3]],
            "outcome": "bank_unverified",
            "message": "指纹库未校准：当前请求条件尚无验证证据；仅展示闭集候选，不判定模型身份"}


def run_round(job, store, accounts, codex_version, probe_fn=probe, *, cancelled=None, prompt_template=None):
    if cancelled is None:
        cancelled = lambda: not store.is_current(job)
    started = time.monotonic()
    budget_seconds = ATTRIBUTION_INITIAL_SECONDS
    deadline = started + budget_seconds
    challenge_count = job.get("challenge_count", 3)
    request_limit = challenge_count * 2
    requests_sent, summaries = 0, []
    run_id = str(uuid.uuid4())
    valid_samples, insufficient_samples = 0, 0
    last_insufficient = ''
    try:
        profile = comparison_profile(job.get("reasoning_effort", "xhigh"))
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
            identities = []
            while len(outputs) < challenge_count and requests_sent < request_limit:
                if cancelled():
                    raise Cancelled()
                if deadline - time.monotonic() < 5:
                    raise RoundTimeout()
                account = accounts.get(job["account_id"])
                ensure_available(account)
                if account_context(account) != context:
                    raise ProbeError("账号路由或配置已变化，本轮作废")
                challenge = next(challenges)
                if prompt_template is not None:
                    challenge = {**challenge, "prompt": prompt_template.replace("{{challenge}}", challenge["prompt"])}
                requests_sent += 1
                try:
                    (output, actual), attempt_id = attempts.invoke(
                        store, job, run_id, account,
                        {"kind": "attribution", "stage": attempt + 1, "sample": requests_sent,
                         "challenge": challenge, "context_sha256": context.hex()},
                        lambda: probe_fn(account, job["model"], challenge, cancelled, deadline, codex_version))
                except InsufficientSample as exc:
                    insufficient_samples += 1
                    last_insufficient = str(exc)
                    if time.monotonic() >= deadline:
                        raise RoundTimeout() from None
                    # Extend once, relative to round start; a replacement never resets time.
                    budget_seconds = ATTRIBUTION_MAX_SECONDS
                    deadline = started + budget_seconds
                    continue
                if time.monotonic() >= deadline:
                    raise RoundTimeout()
                outputs.append(output)
                identities.append(attempt_id)
                valid_samples += 1
            if len(outputs) < challenge_count:
                stage = "初测" if attempt == 0 else "不符复测"
                raise ProbeError(f"{stage}仅取得 {len(outputs)}/{challenge_count} 份有效回答，已达本轮 {request_limit} 次请求上限。{last_insufficient}", "insufficient_samples")
            summary = summarize(outputs, job["model"])
            summary.update(reasoning_effort=job.get("reasoning_effort", "xhigh"), comparison_profile=profile)
            summary["sent_model"] = actual
            summary["attempt_ids"] = identities
            attempts.attach_analysis(store, account, identities, summary)
            summaries.append(summary)
            if attempt == 0 and summary["confident"] and summary["prediction"] != job["model"]:
                if request_limit - requests_sent < challenge_count:
                    summary.update(status="yellow", message=f"初测归因不符，但补采后剩余请求预算不足以完成 {challenge_count} 份独立复测，待确认")
                    break
                budget_seconds = ATTRIBUTION_MAX_SECONDS
                deadline = started + budget_seconds
                continue
            break
        result = dict(summaries[-1])
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
        result = {"status": "yellow", "message": str(exc), "outcome": exc.code}
        if isinstance(exc, RoundTimeout):
            result["message"] = f"检测超时：本轮 {budget_seconds} 秒预算已用尽，取得 {valid_samples} 份有效回答"
    result.update(run_id=run_id, stages=summaries, requests=requests_sent, valid_samples=valid_samples, insufficient_samples=insufficient_samples,
                  challenge_count=challenge_count, request_limit=request_limit,
                  reasoning_effort=job.get("reasoning_effort", "xhigh"), budget_seconds=budget_seconds,
                  initial_budget_seconds=ATTRIBUTION_INITIAL_SECONDS, max_budget_seconds=ATTRIBUTION_MAX_SECONDS,
                  elapsed_seconds=round(time.monotonic() - started, 3))
    return result


def run_prompt_round(job, store, accounts, controller, *, attribution_monitor=False):
    if job.get("test_mode") == "qa":
        return run_qa_round(job, store, accounts, controller)
    cancelled = lambda: not (store.is_current(job) if attribution_monitor else store.is_prompt_current(job))
    started = time.monotonic()
    if cancelled():
        raise Cancelled()
    try:
        validate_prompt_template(job["prompt"])
        controller.check_prompt_support(job.get("reasoning_effort", "xhigh"))
    except ProbeError as exc:
        return {"status": "yellow", "message": str(exc), "outcome": exc.code, "requests": 0,
                "reasoning_effort": job.get("reasoning_effort", "xhigh"),
                "valid_samples": 0, "insufficient_samples": 0,
                "challenge_count": job.get("challenge_count", 3),
                "request_limit": job.get("challenge_count", 3) * 2,
                "latency_ms": int((time.monotonic() - started) * 1000)}

    def gateway_probe(account, model, challenge, cancelled, deadline, _version):
        output = controller.prompt(account["id"], model, challenge["prompt"], cancelled, deadline,
                                   reasoning_effort=job.get("reasoning_effort", "xhigh"))
        minimum = max(80, math.ceil(challenge["expected_count"] * .55))
        count = len(parse_numbers(output))
        attempts.emit("parsed_output", {"output": output, "parsed_count": count, "minimum_count": minimum})
        if count < minimum:
            raise InsufficientSample(f"有效数字样本不足：识别 {count} 个，至少需要 {minimum} 个"
                                     f"（本次要求 {challenge['expected_count']} 个），无法归因")
        return {"text": output, "expected_count": challenge["expected_count"]}, mapped_model(model, account["credentials"].get("model_mapping"))

    result = run_round(job, store, accounts, "", gateway_probe,
                       cancelled=cancelled, prompt_template=job["prompt"])
    result["latency_ms"] = int((time.monotonic() - started) * 1000)
    return result


def run_qa_round(job, store, accounts, controller):
    started = time.monotonic()
    budget_seconds = ATTRIBUTION_INITIAL_SECONDS
    deadline = started + budget_seconds
    cancelled = lambda: not store.is_prompt_current(job)
    requests_sent, results, artifacts = 0, [], {}
    selected = []
    context = None
    run_id = str(uuid.uuid4())

    def check_account():
        if cancelled():
            raise Cancelled()
        if deadline - time.monotonic() < 5:
            raise RoundTimeout()
        account = accounts.get(job["account_id"])
        ensure_available(account, require_bank=False)
        if context is not None and account_context(account) != context:
            raise ProbeError("账号路由或配置已变化，本轮作废")
        return account

    def ask(case, attempt):
        account = check_account()
        output, identity = attempts.invoke(
            store, job, run_id, account,
            {"kind": "qa", "case_id": case["id"], "stage": attempt + 1,
             "prompt": case["prompt"], "expected_answer": case["expected_answer"],
             "context_sha256": context.hex()},
            lambda: controller.prompt(job["account_id"], job["model"], case["prompt"], cancelled, deadline,
                                      reasoning_effort=job.get("reasoning_effort", "xhigh")))
        item["attempts"].append({"attempt_id": identity, "output": output,
                                 "normalized_answer": normalized_answer(output)})
        return output

    try:
        if cancelled():
            raise Cancelled()
        cases = validate_qa_cases(json.loads(job["cases"]), True)
        selected = [case for case in cases if case["enabled"]]
        context = account_context(check_account())
        controller.check_prompt_support(job.get("reasoning_effort", "xhigh"))
        for case in selected:
            item = {"id": case["id"], "name": case["name"], "expected_answer": case["expected_answer"],
                    "status": "yellow", "requests": 0, "kind": case["kind"], "attempts": []}
            results.append(item)
            if case["kind"] == "manual":
                check_account()
                requests_sent += 1
                item["requests"] = 1
                output = ask(case, 0)
                check_account()
                artifacts[case["id"]] = {"html": html_artifact(output), "prompt": case["prompt"]}
                item.update(review_status="pending", artifact_available=True, message="已生成作品，待人工预览评分")
                continue
            answers = []
            for attempt in range(2):
                if attempt == 1:
                    budget_seconds = ATTRIBUTION_MAX_SECONDS
                    deadline = started + budget_seconds
                check_account()
                requests_sent += 1
                item["requests"] += 1
                output = ask(case, attempt)
                check_account()
                answer = normalized_answer(output)
                if not answer:
                    raise ProbeError("模型未返回可判定的文字答案")
                answers.append(answer)
                if "answer_preview" not in item:
                    item["answer_preview"] = output.strip()[:160]
                    item["answer_truncated"] = len(output.strip()) > 160
                if answer == normalized_answer(case["expected_answer"]):
                    item.update(status="green" if attempt == 0 else "yellow",
                                message="答案符合预期" if attempt == 0 else "初测与复测不一致，待确认")
                    break
                if attempt == 1:
                    confirmed = answers[0] == answers[1]
                    item.update(status="red" if confirmed else "yellow",
                                message="两次均返回同一非预期答案" if confirmed else "两次非预期答案不同，待确认")
            # No replacement questions: each selected question runs once, with at most one confirmation.
        check_account()
        status = qa_status(results)
        result = {"status": status, "message": {"green": "所选问答全部符合预期", "red": "有题目复测仍未通过",
                                                "yellow": "本轮含待确认结果"}[status], "_context": context.hex(), "round_complete": True}
    except ProbeError as exc:
        result = {"status": "yellow", "message": str(exc), "outcome": exc.code, "round_complete": False}
        if results and results[-1]["status"] == "yellow":
            results[-1]["message"] = str(exc)
    result.update(run_id=run_id, test_mode="qa", requests=requests_sent, request_limit=sum(1 if case["kind"] == "manual" else 2 for case in selected),
                  _artifacts=artifacts,
                  reasoning_effort=job.get("reasoning_effort", "xhigh"), budget_seconds=budget_seconds,
                  initial_budget_seconds=ATTRIBUTION_INITIAL_SECONDS, max_budget_seconds=ATTRIBUTION_MAX_SECONDS,
                  cases=results, selected_cases=len(selected), latency_ms=int((time.monotonic() - started) * 1000))
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
                    result = run_prompt_round({**job, "test_mode": "attribution", "prompt": DEFAULT_PROMPT},
                                              store, accounts, controller, attribution_monitor=True)
                except Cancelled:
                    continue
                except Exception as exc:
                    LOG.warning("probe failed account_id=%s exception=%s", job["account_id"], type(exc).__name__)
                    result = {"status": "yellow", "outcome": "test_error", "message": "检测暂不可用，等待下一轮"}
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
                    prompt_result = {"status": "yellow", "outcome": "test_error", "message": "提示词测试暂不可用，等待下一轮"}
                store.finish_prompt(prompt_job, prompt_result)
            store.apply_pending_pause(accounts, controller)
            store.apply_pending_prompt_pause(accounts, controller)
        except Exception as exc:
            LOG.warning("scheduler exception=%s", type(exc).__name__)
            stop.wait(15)


def validate_comparison_request(data, correlation_id, account_id, model, prompt, reasoning_effort="xhigh"):
    details = data.get("details") or {}
    body = details.get("body") or {}
    if (data.get("client_request_id") != correlation_id or data.get("profile") != comparison_profile(reasoning_effort)
            or details.get("account_id") != account_id
            or details.get("transport") != "codex-http1-openssl36"
            or body.get("model") != model or body.get("reasoning") != {"effort": reasoning_effort}
            or body.get("instructions") != COMPARISON_INSTRUCTIONS
            or body.get("tools") != [] or body.get("tool_choice") != "none"
            or body.get("parallel_tool_calls") is not False or body.get("text") != {"verbosity": "medium"}):
        raise ProbeError("网关比较条件或请求关联信息不一致")
    context = []
    matched = 0
    for item in body.get("input", []):
        if (item.get("role") == "user" and item.get("content") == [{"type": "input_text", "text": prompt}]):
            matched += 1
        else:
            context.append(item)
    if matched != 1:
        raise ProbeError("网关没有原样发送指定题目")
    # Per-request session identifiers are intentionally fresh; keep every semantic field.
    frozen = {k: v for k, v in body.items() if k not in ("input", "client_metadata", "prompt_cache_key")}
    ephemeral = {"session_id", "thread_id", "turn_id", "root_turn_id", "x-codex-window-id"}
    metadata = {k: v for k, v in body.get("client_metadata", {}).items() if k not in ephemeral}
    if "x-codex-turn-metadata" in metadata:
        try:
            turn = json.loads(metadata["x-codex-turn-metadata"])
            metadata["x-codex-turn-metadata"] = {k: v for k, v in turn.items()
                                                  if k not in ephemeral | {"window_id"}}
        except (TypeError, ValueError, AttributeError):
            raise ProbeError("网关客户端元数据无效") from None
    frozen["client_metadata"] = metadata
    frozen["context"] = context
    headers = {k.lower(): v for k, v in details.get("headers", {}).items()
               if k.lower() in ("version", "user-agent", "originator", "openai-beta", "x-codex-beta-features")}
    contract = {"body": frozen, "headers": headers, "transport": details["transport"],
                "proxy_id": details.get("proxy_id"), "url": details.get("url")}
    try:
        attempts.compare_contract(contract)
    except ValueError:
        raise ProbeError("本轮请求模型、档位、指令、环境或传输条件已变化，比较作废") from None
    attempts.emit("gateway_request", data)


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

    def check_prompt_support(self, reasoning_effort="xhigh"):
        profile = comparison_profile(reasoning_effort)
        try:
            with requests.Session() as session:
                session.trust_env = False
                response = session.get(self.base_url + "/api/v1/admin/accounts/test-capabilities",
                                       headers={"X-API-Key": self._key()}, timeout=(1, 3), allow_redirects=False)
                if response.status_code == 200:
                    payload = response.json()
                    data = payload.get("data") or {}
                    if (payload.get("code") == 0 and data.get("comparison_audit_v1") is True
                            and profile in data.get("comparison_profiles", [])):
                        return
        except (requests.RequestException, ValueError, AttributeError):
            pass
        raise ProbeError(f"GN2API 尚未支持 {reasoning_effort} 档的固定条件比较接口，本轮未发送模型请求；请先安装接口修复", "capability_unavailable")

    def prompt(self, account_id, model, prompt, cancelled=lambda: False, deadline=None, *, reasoning_effort="xhigh"):
        profile = comparison_profile(reasoning_effort)
        if type(account_id) is not int or account_id <= 0:
            raise ProbeError("账号 ID 无效")
        if not isinstance(model, str) or not model or len(model) > 160:
            raise ProbeError("测试模型无效")
        if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > PROMPT_MAX_CHARS + 2000:
            raise ProbeError("测试提示词无效")
        key = self._key()
        pieces, total, complete = [], 0, False
        seen_request, seen_response = False, False
        upstream_attempts = set()
        deadline = deadline if deadline is not None else time.monotonic() + ATTRIBUTION_INITIAL_SECONDS
        remaining = deadline - time.monotonic()
        if cancelled():
            raise Cancelled()
        if remaining < 5:
            raise RoundTimeout()
        correlation_id = attempts.ACTIVE.get()["id"] if attempts.ACTIVE.get() else str(uuid.uuid4())
        audit_headers = {"X-Sub2API-Comparison-Profile": profile, "X-Client-Request-Id": correlation_id}
        attempts.request_snapshot(f"{self.base_url}/api/v1/admin/accounts/{account_id}/test",
                                  audit_headers, {"model_id": model, "prompt": prompt, "mode": "attribution"}, "sub2api-admin-sse")
        try:
            with requests.Session() as session:
                session.trust_env = False
                with session.post(
                    f"{self.base_url}/api/v1/admin/accounts/{account_id}/test",
                    headers={"X-API-Key": key, "Accept": "text/event-stream", **audit_headers},
                    json={"model_id": model, "prompt": prompt, "mode": "attribution"},
                    stream=True, allow_redirects=False, timeout=(min(8, remaining), min(PROBE_READ_TIMEOUT, remaining)),
                ) as response:
                    attempts.response_snapshot(response)
                    if response.status_code != 200:
                        attempts.error_body(response, MAX_RESPONSE_BYTES)
                        raise ProbeError("账号测试接口返回错误")
                    for raw in response.iter_lines(chunk_size=256):
                        attempts.stream_line(raw, limit=MAX_RESPONSE_BYTES)
                        if cancelled():
                            raise Cancelled()
                        if time.monotonic() >= deadline:
                            raise RoundTimeout()
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
                        if event_type == "test_request":
                            data = event.get("data") or {}
                            validate_comparison_request(data, correlation_id, account_id, model, prompt, reasoning_effort)
                            upstream_id = data.get("upstream_attempt_id")
                            if not isinstance(upstream_id, str) or not upstream_id:
                                raise ProbeError("网关缺少上游尝试关联 ID")
                            upstream_attempts.add(upstream_id)
                            seen_request = True
                        elif event_type == "test_response":
                            data = event.get("data") or {}
                            if (data.get("client_request_id") != correlation_id or data.get("profile") != profile
                                    or data.get("upstream_attempt_id") not in upstream_attempts):
                                raise ProbeError("网关响应与请求尝试不匹配")
                            seen_response = (data.get("details") or {}).get("http_status") == 200
                        elif event_type == "content":
                            text = event.get("text")
                            if isinstance(text, str):
                                pieces.append(text)
                        elif event_type == "error":
                            raise ProbeError("上游返回处理错误，本轮未完成；详细响应已保存在本轮尝试记录", "upstream_error")
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
            if time.monotonic() >= deadline:
                raise RoundTimeout() from None
            raise ProbeError(transport_failure_message(exc, min(PROBE_READ_TIMEOUT, remaining))) from None
        if not seen_request or not seen_response:
            raise ProbeError("账号测试缺少完整请求关联信息")
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
    app.config["MAX_CONTENT_LENGTH"] = 128 * 1024
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
        prompt = result["prompt_test"]
        prompt["available"] = not prompt_unavailable(meta, prompt["test_mode"])
        prompt["reason"] = prompt_unavailable(meta, prompt["test_mode"])
        prompt["qa_available"] = not prompt_unavailable(meta, "qa")
        prompt["qa_reason"] = prompt_unavailable(meta, "qa")
        prompt["attribution_available"] = not prompt_unavailable(meta)
        prompt["attribution_reason"] = prompt_unavailable(meta)
        return result

    @app.get("/accounts/<int:account_id>/attempts")
    def list_attempts(account_id):
        before = request.args.get("before", type=int)
        return jsonify(attempts.listing(store, account_id, before, request.args.get("run_id")))

    @app.get("/accounts/<int:account_id>/attempts/<attempt_id>")
    def get_attempt(account_id, attempt_id):
        result = attempts.detail(store, account_id, attempt_id)
        return jsonify(result) if result else (jsonify(error="尝试记录不存在"), 404)

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
        reasoning_effort = body.get("reasoning_effort", previous["reasoning_effort"] if previous else "xhigh")
        if reasoning_effort not in REASONING_EFFORTS:
            return jsonify(error="推理等级请选择 low、medium、high 或 xhigh"), 400
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
                               and previous["reasoning_effort"] == reasoning_effort
                               and not auto_pause and threshold == previous["auto_pause_threshold"])
            if unavailable(meta) and not cancelling_only:
                return jsonify(error=unavailable(meta)), 400
            if model not in model_choices(meta) and not cancelling_only:
                return jsonify(error="请选择指纹库已收录且账号支持的模型"), 400
        store.configure(account_id, body["enabled"], model, auto_pause, threshold, interval_minutes, challenge_count, reasoning_effort)
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
        reasoning_effort = body.get("reasoning_effort", previous["reasoning_effort"] if previous else "xhigh")
        if reasoning_effort not in REASONING_EFFORTS:
            return jsonify(error="推理等级请选择 low、medium、high 或 xhigh"), 400
        test_mode = body.get("test_mode", previous["test_mode"] if previous else "attribution")
        if test_mode not in ("attribution", "qa"):
            return jsonify(error="请选择问答测试或数字归因模板"), 400
        cases = body.get("cases", json.loads(previous["cases"]) if previous else [])
        if test_mode == "qa":
            try:
                cases = validate_qa_cases(cases, body["enabled"])
            except ProbeError as exc:
                return jsonify(error=str(exc)), 400
        elif previous:
            cases = json.loads(previous["cases"])
        else:
            cases = []
        model = body.get("model", previous["model"] if previous else "gpt-6-astra")
        prompt = body.get("prompt", previous["prompt"] if previous else DEFAULT_PROMPT)
        auto_pause = body.get("auto_pause_enabled", bool(previous and previous["auto_pause_enabled"]))
        threshold = body.get("auto_pause_threshold", previous["auto_pause_threshold"] if previous else 3)
        interval_minutes = body.get("interval_minutes", previous["interval_minutes"] if previous else 10)
        challenge_count = body.get("challenge_count", previous["challenge_count"] if previous else 3)
        if not isinstance(model, str) or not model.strip() or len(model) > 160:
            return jsonify(error="测试模型无效"), 400
        if body["enabled"] and test_mode == "attribution":
            try:
                validate_prompt_template(prompt)
            except ProbeError as exc:
                return jsonify(error=str(exc)), 400
        elif previous:
            prompt = previous["prompt"]
        elif test_mode == "qa":
            prompt = DEFAULT_PROMPT
        if type(challenge_count) is not int or challenge_count not in (1, 2, 3):
            return jsonify(error="挑战次数请选择 1、2 或 3 次"), 400
        if type(auto_pause) is not bool or type(threshold) is not int or not 1 <= threshold <= 100:
            return jsonify(error="自动停调度开关无效，或连续红点轮数不在 1–100 之间"), 400
        if type(interval_minutes) is not int or not 1 <= interval_minutes <= 1440:
            return jsonify(error="测试间隔请填写 1–1440 分钟的整数"), 400
        if body["enabled"]:
            if prompt_unavailable(meta, test_mode):
                return jsonify(error=prompt_unavailable(meta, test_mode)), 400
            if test_mode == "attribution" and model.strip() not in model_choices(meta):
                return jsonify(error="请选择指纹库已收录且账号支持的模型"), 400
            mapping = meta.get("mapping") or {}
            if test_mode == "qa" and mapping and not any(
                re.fullmatch(re.escape(key).replace(r"\*", ".*"), model.strip()) for key in mapping
            ):
                return jsonify(error="请选择此账号支持的模型"), 400
        store.configure_prompt(account_id, body["enabled"], model.strip(), prompt,
                               auto_pause, threshold, interval_minutes, challenge_count, test_mode, cases, reasoning_effort)
        return jsonify(account=render(account_id, meta))

    @app.get("/accounts/<int:account_id>/prompt-test/results/<int:result_id>/artifacts/<case_id>")
    def get_artifact(account_id, result_id, case_id):
        artifact = store.artifact(account_id, result_id, case_id)
        if artifact is None:
            return jsonify(error="作品不存在或已随历史记录清理"), 404
        return jsonify(artifact)

    @app.put("/accounts/<int:account_id>/prompt-test/results/<int:result_id>/reviews/<case_id>")
    def review_artifact(account_id, result_id, case_id):
        body = request.get_json(silent=True)
        if not isinstance(body, dict) or body.get("status") not in ("green", "yellow", "red"):
            return jsonify(error="请选择绿、黄或红作为人工评分"), 400
        meta = accounts.metadata([account_id]).get(account_id)
        if not meta:
            return jsonify(error="账号不存在"), 404
        if not store.review_artifact(account_id, result_id, case_id, body["status"]):
            return jsonify(error="可评分作品不存在或已随历史记录清理"), 404
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
