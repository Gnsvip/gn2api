"""Durable, append-only probe evidence, independent of the recent-results window."""
from contextvars import ContextVar
import base64
import hashlib
import json
import time
import uuid

ACTIVE = ContextVar("modeltrace_attempt", default=None)
SAFE_HEADERS = {"content-type", "user-agent", "originator", "version", "openai-beta",
                "x-request-id", "request-id", "x-client-request-id", "cf-ray",
                "x-codex-turn-state", "x-codex-beta-features", "x-sub2api-comparison-profile"}


def initialize(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS probe_attempts (
            sequence INTEGER PRIMARY KEY, attempt_id TEXT NOT NULL UNIQUE,
            run_id TEXT NOT NULL, account_id INTEGER NOT NULL, started_at REAL NOT NULL,
            metadata TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS probe_attempts_account ON probe_attempts(account_id, sequence);
        CREATE INDEX IF NOT EXISTS probe_attempts_run ON probe_attempts(account_id, run_id);
        CREATE TABLE IF NOT EXISTS probe_attempt_events (
            sequence INTEGER PRIMARY KEY, attempt_id TEXT NOT NULL,
            at REAL NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS probe_events_attempt ON probe_attempt_events(attempt_id, sequence);
        CREATE UNIQUE INDEX IF NOT EXISTS probe_events_finished ON probe_attempt_events(attempt_id)
            WHERE kind='finished';
    """)


def emit(kind, payload):
    active = ACTIVE.get()
    if active is None:
        return
    encoded = json.dumps(payload, ensure_ascii=False, default=str)
    for secret in active["secrets"]:
        if secret:
            encoded = encoded.replace(json.dumps(secret, ensure_ascii=False)[1:-1], "[REDACTED]")
    with active["store"].db() as db:
        db.execute("INSERT INTO probe_attempt_events(attempt_id,at,kind,payload) VALUES(?,?,?,?)",
                   (active["id"], time.time(), kind, encoded))


def request_snapshot(url, headers, payload, protocol):
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    emit("request", {"url": url, "protocol": protocol, "body": payload,
                     "body_sha256": hashlib.sha256(encoded).hexdigest(),
                     "headers": {k: v for k, v in headers.items() if k.lower() in SAFE_HEADERS}})


def response_snapshot(response):
    if ACTIVE.get() is None:
        return
    emit("response", {"http_status": response.status_code,
                      "headers": {k: v for k, v in response.headers.items() if k.lower() in SAFE_HEADERS}})


def capture_bytes(kind, raw, *, limit=None, more=False):
    # Persist before parsing, including malformed/error/partial SSE. Store exact bytes
    # unless they contain a known credential, which is deliberately redacted.
    active = ACTIVE.get()
    if active is None:
        return
    received = len(raw)
    truncated = more or (limit is not None and received > limit)
    if limit is not None:
        raw = raw[:limit]
    for secret in active["secrets"]:
        if secret:
            raw = raw.replace(secret.encode(), b"[REDACTED]")
    emit(kind, {"line_base64": base64.b64encode(raw).decode(),
                "line": raw.decode("utf-8", errors="replace"),
                "received_bytes": received, "captured_bytes": len(raw), "truncated": truncated})


def stream_line(raw, limit=None):
    capture_bytes("stream", raw, limit=limit)


def error_body(response, limit):
    if ACTIVE.get() is None:
        return
    data = bytearray()
    try:
        for chunk in response.iter_content(chunk_size=8192):
            data.extend(chunk[:max(0, limit + 1 - len(data))])
            if len(data) > limit:
                break
    finally:
        capture_bytes("error_body", bytes(data), limit=limit)


def invoke(store, job, run_id, account, metadata, operation):
    attempt_id = str(uuid.uuid4())
    started = time.time()
    metadata = {**metadata, "requested_model": job["model"], "generation": job["generation"],
                "run_sequence": job.get("run_sequence"), "proxy_id": account.get("proxy_id"),
                "timezone": (account.get("extra") or {}).get("codex_timezone")}
    credentials = account.get("credentials") or {}
    secrets = [credentials.get(k) for k in ("access_token", "refresh_token", "api_key")]
    secrets += [account.get("proxy_password")]
    with store.db() as db:
        db.execute("INSERT INTO probe_attempts(attempt_id,run_id,account_id,started_at,metadata) VALUES(?,?,?,?,?)",
                   (attempt_id, run_id, job["account_id"], started, json.dumps(metadata, ensure_ascii=False)))
    token = ACTIVE.set({"store": store, "id": attempt_id, "run_id": run_id, "secrets": secrets})
    try:
        value = operation()
        result = value[0] if isinstance(value, tuple) else value
        text = result.get("text", "") if isinstance(result, dict) else str(result)
        emit("finished", {"status": "completed", "output": text,
                          "result": value, "latency_ms": int((time.time() - started) * 1000)})
        return value, attempt_id
    except BaseException as error:
        emit("finished", {"status": "cancelled" if type(error).__name__ == "Cancelled" else "failed",
                          "error_type": type(error).__name__,
                          "error": str(error) if type(error).__name__ in ("ProbeError", "InsufficientSample") else type(error).__name__,
                          "latency_ms": int((time.time() - started) * 1000)})
        raise
    finally:
        ACTIVE.reset(token)


def attach_analysis(store, account, identities, payload):
    credentials = account.get("credentials") or {}
    secrets = [credentials.get(k) for k in ("access_token", "refresh_token", "api_key")]
    secrets.append(account.get("proxy_password"))
    for identity in identities:
        token = ACTIVE.set({"store": store, "id": identity, "secrets": secrets})
        try:
            emit("analysis", payload)
        finally:
            ACTIVE.reset(token)


def listing(store, account_id, before=None, run_id=None):
    clauses, params = ["a.account_id=?"], [account_id]
    if before is not None:
        clauses.append("a.sequence<?")
        params.append(before)
    if run_id:
        clauses.append("a.run_id=?")
        params.append(run_id)
    with store.db() as db:
        rows = db.execute("SELECT a.*,e.payload AS finished FROM probe_attempts a LEFT JOIN probe_attempt_events e "
                          "ON e.attempt_id=a.attempt_id AND e.kind='finished' WHERE " + " AND ".join(clauses) +
                          " ORDER BY a.sequence DESC LIMIT 21", params).fetchall()
    items = [{**dict(r), "metadata": json.loads(r["metadata"]),
              "finished": json.loads(r["finished"]) if r["finished"] else {"status": "unfinished"}} for r in rows[:20]]
    return {"attempts": items, "next_cursor": items[-1]["sequence"] if len(rows) > 20 else None}


def compare_contract(contract):
    """Fail closed if a later sample silently changes the frozen request conditions."""
    active = ACTIVE.get()
    encoded = json.dumps(contract, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    if active is None:
        return
    with active["store"].db() as db:
        first = db.execute("SELECT e.payload FROM probe_attempt_events e JOIN probe_attempts a "
                           "ON a.attempt_id=e.attempt_id WHERE a.run_id=? AND e.kind='comparison_contract' "
                           "ORDER BY e.sequence LIMIT 1", (active["run_id"],)).fetchone()
    emit("comparison_contract", {"sha256": digest, "contract": contract})
    if first and json.loads(first[0])["sha256"] != digest:
        raise ValueError("comparison conditions changed within the run")


def detail(store, account_id, attempt_id):
    with store.db() as db:
        row = db.execute("SELECT * FROM probe_attempts WHERE account_id=? AND attempt_id=?",
                         (account_id, attempt_id)).fetchone()
        if not row:
            return None
        events = db.execute("SELECT at,kind,payload FROM probe_attempt_events WHERE attempt_id=? ORDER BY sequence",
                            (attempt_id,)).fetchall()
    return {**dict(row), "metadata": json.loads(row["metadata"]),
            "events": [{"at": e["at"], "kind": e["kind"], "payload": json.loads(e["payload"])} for e in events]}
