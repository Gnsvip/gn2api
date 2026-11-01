"""Keep one baseline and one first anomaly per account and comparison condition."""
from contextvars import ContextVar
from functools import wraps
import hashlib
import json
import time
from urllib.parse import urlsplit, urlunsplit

ROUND = ContextVar('modeltrace_evidence_round', default=None)
PRIVATE_KEYS = {'authorization', 'proxy-authorization', 'cookie', 'set-cookie',
                'access_token', 'refresh_token', 'api_key', 'password', 'proxy_password', 'x-api-key'}


def clean(value, secrets=()):
    def scrub(item):
        if isinstance(item, dict):
            return {k: safe_url(v) if k == 'url' and isinstance(v, str) else scrub(v)
                    for k, v in item.items() if k.lower() not in PRIVATE_KEYS}
        if isinstance(item, (list, tuple)):
            return [scrub(v) for v in item]
        return item
    encoded = json.dumps(scrub(value), ensure_ascii=False, default=str)
    for secret in secrets:
        if secret:
            encoded = encoded.replace(json.dumps(secret, ensure_ascii=False)[1:-1], '[REDACTED]')
    return json.loads(encoded)


def safe_url(url):
    parsed = urlsplit(url)
    host = parsed.hostname or ''
    if ':' in host:
        host = '[' + host + ']'
    if parsed.port:
        host += ':' + str(parsed.port)
    return urlunsplit((parsed.scheme, host, parsed.path, '', ''))


def capture_round(operation):
    @wraps(operation)
    def run(*args, **kwargs):
        captures = []
        token = ROUND.set(captures)
        try:
            result = operation(*args, **kwargs)
            result['_evidence'] = captures
            return result
        finally:
            ROUND.reset(token)
    return run


def initialize(db):
    db.execute('''CREATE TABLE IF NOT EXISTS probe_evidence (
        id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL, condition_sha256 TEXT NOT NULL,
        slot TEXT NOT NULL CHECK(slot IN ('normal','anomaly')), created_at REAL NOT NULL,
        run_id TEXT NOT NULL, summary TEXT NOT NULL,
        UNIQUE(account_id, condition_sha256, slot))''')


def begin(identity, metadata, secrets):
    captures = ROUND.get()
    if captures is None:
        return None
    record = {'attempt_id': identity, 'metadata': clean(metadata, secrets), 'events': []}
    captures.append(record)
    return record


def record(active, kind, payload):
    capture = active.get('capture')
    if capture is not None:
        capture['events'].append({'at': time.time(), 'kind': kind, 'payload': clean(payload, active['secrets'])})


def condition(job, records, case, result):
    metadata = records[0]['metadata']
    requests = [event['payload'] for record in records for event in record['events']
                if event['kind'] == 'gateway_request']
    direct = [event['payload'] for record in records for event in record['events'] if event['kind'] == 'request']
    source = 'gateway-admin-http' if requests else ('direct-' + direct[0]['protocol'] if direct else 'unavailable')
    # Proxy, timezone, client identity and observed transport remain in the evidence:
    # changing these must not hide the anomaly behind a new baseline.
    value = {'schema': 1, 'kind': metadata['kind'], 'model': job['model'],
             'sampling_policy': result.get('sampling_policy', 'legacy-retest'),
             'reasoning_effort': job.get('reasoning_effort', 'xhigh'), 'source': source,
             'comparison_profile': requests[0].get('profile') if requests else None}
    if case:
        value.update(prompt=metadata.get('prompt'), expected_answer=metadata.get('expected_answer'))
    else:
        value.update(template=metadata.get('template'), challenge_count=job.get('challenge_count', 3),
                     bank_sha256=next((s.get('bank_sha256') for s in result.get('stages', []) if s.get('bank_sha256')), None))
    digest = hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return digest, source


def commit(db, job, result):
    """Called inside the result transaction, after generation/run validation."""
    records = result.pop('_evidence', [])
    selected = set()
    if result.get('outcome') == 'comparison_changed':
        records = []
    groups = []
    if result.get('test_mode') == 'qa':
        for case in result.get('cases', []):
            if case.get('kind') == 'manual' or not case.get('comparison_observed'):
                continue
            matching = [r for r in records if r['metadata'].get('case_id') == case['id']]
            if matching:
                groups.append((matching, case, 'normal' if case['status'] == 'green' else 'anomaly'))
    else:
        stages = result.get('stages', [])
        mismatches = [s for s in stages if (s.get('candidate_separated') or s.get('confident'))
                      and s.get('prediction') != job['model']]
        matching = stages and all((s.get('candidate_separated') or s.get('confident'))
                                  and s.get('prediction') == job['model'] for s in stages)
        if records and (mismatches or matching):
            groups.append((records, None, 'anomaly' if mismatches else 'normal'))
    for captured, case, slot in groups:
        digest, source = condition(job, captured, case, result)
        summary = {'kind': captured[0]['metadata']['kind'], 'case_id': case['id'] if case else None,
                   'model': job['model'], 'reasoning_effort': job.get('reasoning_effort', 'xhigh'),
                   'source': source, 'status': case['status'] if case else result.get('status'),
                   'message': case.get('message') if case else result.get('message'),
                   'prediction': result.get('prediction', (result.get('stages') or [{}])[-1].get('prediction')),
                   'bank_applicability': result.get('bank_applicability', (result.get('stages') or [{}])[-1].get('bank_applicability')),
                   'attempt_ids': [r['attempt_id'] for r in captured]}
        inserted = db.execute('''INSERT INTO probe_evidence(account_id,condition_sha256,slot,created_at,run_id,summary)
            VALUES(?,?,?,?,?,?) ON CONFLICT(account_id,condition_sha256,slot) DO NOTHING''',
            (job['account_id'], digest, slot, time.time(), result['run_id'], json.dumps(summary, ensure_ascii=False)))
        if not inserted.rowcount:
            continue
        for record in captured:
            selected.add(record['attempt_id'])
            db.execute('INSERT INTO probe_attempt_events(attempt_id,at,kind,payload) VALUES(?,?,?,?)',
                       (record['attempt_id'], time.time(), 'diagnostic', json.dumps(record, ensure_ascii=False)))
            db.execute("UPDATE probe_attempts SET metadata=json_set(metadata,'$.capture_slot',?,'$.condition_sha256',?) WHERE attempt_id=?",
                       (slot, digest, record['attempt_id']))
    # History stays small. Full answers are read on demand from the selected record.
    for case in result.get('cases', []):
        case['attempts'] = [{**{k: v for k, v in a.items() if k not in ('output', 'normalized_answer')},
                             'output': a.get('output', '')[:160], 'output_chars': len(a.get('output', '')),
                             'output_truncated': len(a.get('output', '')) > 160,
                             'evidence_saved': a.get('attempt_id') in selected} for a in case.get('attempts', [])]


def listing(store, account_id):
    with store.db() as db:
        rows = db.execute('SELECT * FROM probe_evidence WHERE account_id=? ORDER BY id DESC', (account_id,)).fetchall()
    return [{**dict(row), 'summary': json.loads(row['summary'])} for row in rows]


def answer(store, account_id, attempt_id):
    with store.db() as db:
        rows = db.execute('''SELECT e.kind,e.payload FROM probe_attempt_events e JOIN probe_attempts a
            ON a.attempt_id=e.attempt_id WHERE a.account_id=? AND a.attempt_id=? AND e.kind IN ('diagnostic','finished')
            ORDER BY CASE e.kind WHEN 'diagnostic' THEN 0 ELSE 1 END''', (account_id, attempt_id)).fetchall()
    for row in rows:
        payload = json.loads(row['payload'])
        if row['kind'] == 'diagnostic':
            payload = next((e['payload'] for e in payload['events'] if e['kind'] == 'finished'), {})
        if 'output' in payload:
            return {k: payload[k] for k in ('output', 'output_truncated') if k in payload}
    return None
