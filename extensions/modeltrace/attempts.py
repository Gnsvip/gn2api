"""Lightweight attempt history with bounded baseline/anomaly evidence."""
from contextvars import ContextVar
import evidence
import hashlib
import json
import time
import uuid

ACTIVE = ContextVar("modeltrace_attempt", default=None)
SAFE_HEADERS = {"x-request-id", "request-id", "x-client-request-id"}


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
    evidence.initialize(db)


def emit(kind, payload):
    # Never collect SSE fragments or duplicate them as Base64.
    if kind in ('stream', 'error_body'):
        return
    active = ACTIVE.get()
    if active is None:
        return
    if kind == 'parsed_output':
        active['parsed_output'] = payload
        return
    evidence.record(active, kind, payload)
    if kind == 'request':
        payload = {k: v for k, v in payload.items() if k not in ('body', 'url', 'headers')}
    if kind == 'comparison_contract':
        payload = {'sha256': payload['sha256']}
    if kind == 'finished' and 'output' in payload:
        payload = {**payload, 'output': payload['output'][:160], 'output_chars': len(payload['output']),
                   'output_truncated': len(payload['output']) > 160}
    if kind in ('gateway_request', 'gateway_response'):
        details = payload.get('details') or {}
        payload = {k:payload[k] for k in ('client_request_id', 'upstream_attempt_id', 'profile') if k in payload}
        payload['details'] = {k:details[k] for k in ('account_id', 'transport', 'proxy_id', 'http_status') if k in details}
        payload['details']['headers'] = {k:v for k,v in details.get('headers', {}).items() if k.lower() in SAFE_HEADERS}
    encoded = json.dumps(evidence.clean(payload, active['secrets']), ensure_ascii=False)
    with active["store"].db() as db:
        db.execute("INSERT INTO probe_attempt_events(attempt_id,at,kind,payload) VALUES(?,?,?,?)",
                   (active["id"], time.time(), kind, encoded))


def request_snapshot(url, headers, payload, protocol):
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    emit("request", {"protocol": protocol, "model": payload.get('model') or payload.get('model_id'),
                     "reasoning_effort": (payload.get('reasoning') or {}).get('effort'),
                     "body_sha256": hashlib.sha256(encoded).hexdigest(),
                     "url": evidence.safe_url(url), "body": payload, "headers": headers})


def response_snapshot(response):
    if ACTIVE.get() is None:
        return
    emit("response", {"http_status": response.status_code,
                      "headers": {k: v for k, v in response.headers.items() if k.lower() in SAFE_HEADERS}})


def stream_line(raw, limit=None):
    # Compatibility hook for parsers: bytes are processed in memory only.
    pass


def error_body(response, limit):
    # HTTP status and the classified failure are enough; do not copy error bodies.
    pass


def invoke(store, job, run_id, account, metadata, operation):
    attempt_id = str(uuid.uuid4())
    started = time.time()
    metadata = {**metadata, "requested_model": job["model"], "generation": job["generation"],
                "reasoning_effort": job.get("reasoning_effort", "xhigh"),
                "run_sequence": job.get("run_sequence"), "proxy_id": account.get("proxy_id"),
                "proxy_endpoint": {k: account.get('proxy_' + k) for k in ('protocol', 'host', 'port')},
                "timezone": (account.get("extra") or {}).get("codex_timezone"), "capture_mode": "first-normal-anomaly-v1"}
    credentials = account.get("credentials") or {}
    secrets = [credentials.get(k) for k in ("access_token", "refresh_token", "api_key")]
    secrets += [account.get("proxy_password")]
    capture = evidence.begin(attempt_id, metadata, secrets)
    light_metadata = {k: v for k, v in metadata.items() if k not in ('prompt', 'challenge', 'template', 'expected_answer')}
    with store.db() as db:
        db.execute("INSERT INTO probe_attempts(attempt_id,run_id,account_id,started_at,metadata) VALUES(?,?,?,?,?)",
                   (attempt_id, run_id, job["account_id"], started, json.dumps(evidence.clean(light_metadata, secrets), ensure_ascii=False)))
    token = ACTIVE.set({"store": store, "id": attempt_id, "run_id": run_id, "secrets": secrets, 'capture': capture})
    try:
        value = operation()
        result = value[0] if isinstance(value, tuple) else value
        text = result.get("text", "") if isinstance(result, dict) else str(result)
        emit("finished", {"status": "completed", "output": text,
                          "latency_ms": int((time.time() - started) * 1000)})
        return value, attempt_id
    except BaseException as error:
        probe_error = type(error).__name__ in ("ProbeError", "InsufficientSample", "RoundTimeout")
        parsed = (ACTIVE.get() or {}).get('parsed_output') or {}
        emit("finished", {"status": "cancelled" if type(error).__name__ == "Cancelled" else "failed",
                          "error_type": type(error).__name__,
                          "error": str(error) if probe_error else type(error).__name__,
                          "error_code": error.code if probe_error else None,
                          "latency_ms": int((time.time() - started) * 1000),
                          **{k:parsed[k] for k in ('output', 'parsed_count', 'minimum_count') if k in parsed}})
        raise
    finally:
        ACTIVE.reset(token)


def attach_analysis(store, account, identities, payload):
    credentials = account.get("credentials") or {}
    secrets = [credentials.get(k) for k in ("access_token", "refresh_token", "api_key")]
    secrets.append(account.get("proxy_password"))
    for identity in identities:
        capture = next((r for r in evidence.ROUND.get() or [] if r['attempt_id'] == identity), None)
        token = ACTIVE.set({"store": store, "id": identity, "secrets": secrets, 'capture': capture})
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
        rows = db.execute("SELECT a.*,json_object('status',coalesce(json_extract(e.payload,'$.status'),'unfinished'),"
                          "'error',substr(json_extract(e.payload,'$.error'),1,300),"
                          "'error_code',json_extract(e.payload,'$.error_code'),"
                          "'latency_ms',json_extract(e.payload,'$.latency_ms'),"
                          "'output_chars',coalesce(json_extract(e.payload,'$.output_chars'),length(json_extract(e.payload,'$.output')))) AS finished "
                          "FROM probe_attempts a LEFT JOIN probe_attempt_events e "
                          "ON e.attempt_id=a.attempt_id AND e.kind='finished' WHERE " + " AND ".join(clauses) +
                          " ORDER BY a.sequence DESC LIMIT 21", params).fetchall()
    items = [{**dict(r), "metadata": {k:v for k,v in json.loads(r["metadata"]).items()
              if k in ('kind', 'case_id', 'stage', 'sample', 'requested_model', 'reasoning_effort',
                       'generation', 'run_sequence', 'proxy_id', 'timezone', 'capture_mode', 'capture_slot', 'condition_sha256')},
              "finished": json.loads(r["finished"]) if r["finished"] else {"status": "unfinished"}} for r in rows[:20]]
    return {"attempts": items, "next_cursor": items[-1]["sequence"] if len(rows) > 20 else None,
            **({'saved_evidence': evidence.listing(store, account_id)} if before is None and not run_id else {})}


def event_page(store, account_id, attempt_id, after=0, include_stream=False):
    """Bound browser work even when one response contains thousands of SSE events."""
    with store.db() as db:
        row = db.execute("SELECT * FROM probe_attempts WHERE account_id=? AND attempt_id=?",
                         (account_id, attempt_id)).fetchone()
        if not row:
            return None
        filter_sql = '' if include_stream else " AND kind!='stream'"
        total = db.execute("SELECT count(*) FROM probe_attempt_events WHERE attempt_id=?", (attempt_id,)).fetchone()[0]
        count = db.execute("SELECT count(*) FROM probe_attempt_events WHERE attempt_id=?" + filter_sql, (attempt_id,)).fetchone()[0]
        events = db.execute("SELECT sequence,at,kind,length(payload) AS payload_chars,substr(payload,1,2000) AS preview "
                            "FROM probe_attempt_events WHERE attempt_id=? AND sequence>?" + filter_sql + " ORDER BY sequence LIMIT 41",
                            (attempt_id, after)).fetchall()
    return {"attempt_id": attempt_id, "run_id": row["run_id"], "event_count": count, "total_event_count": total,
            "events": [{**dict(e), "truncated": e["payload_chars"] > 2000} for e in events[:40]],
            "next_cursor": events[39]["sequence"] if len(events) > 40 else None}


def export(store, account_id, attempt_id):
    """Stream the unchanged full evidence; the browser downloads it without JSON/DOM expansion."""
    with store.db() as db:
        row = db.execute("SELECT * FROM probe_attempts WHERE account_id=? AND attempt_id=?",
                         (account_id, attempt_id)).fetchone()
        if not row:
            return None
    header = {**dict(row), "metadata": json.loads(row["metadata"])}

    def chunks():
        yield json.dumps(header, ensure_ascii=False)[:-1] + ', "events": ['
        with store.db() as db:
            # One SQLite read snapshot keeps this export internally consistent.
            cursor = db.execute("SELECT at,kind,payload FROM probe_attempt_events WHERE attempt_id=? ORDER BY sequence",
                                (attempt_id,))
            for i, event in enumerate(cursor):
                yield (',' if i else '') + json.dumps({'at':event['at'], 'kind':event['kind']})[:-1] + ', "payload":' + event['payload'] + '}'
        yield ']}'
    return chunks()


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
    emit("comparison_contract", {"sha256": digest, 'contract': contract})
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
