import json
import time

import pytest

import attempts
import service as s
from test_service import FakeAccounts, claim, prompt_claim, store
from test_qa import Answers, configure, question


def test_retest_preserves_both_answers_and_full_output(store):
    configure(store)
    job = prompt_claim(store)
    result = s.run_prompt_round(job, store, FakeAccounts(), Answers("29", "21"))
    store.finish_prompt(job, result)
    case = result["cases"][0]
    assert case["answer_preview"] == "29"
    assert [a["output"] for a in case["attempts"]] == ["29", "21"]
    saved = attempts.listing(store, 7, run_id=result["run_id"])["attempts"]
    records = [attempts.detail(store, 7, a["attempt_id"]) for a in saved]
    assert [next(e['payload']['output'] for e in r['events'] if e['kind'] == 'finished') for r in records] == ['21', '29']
    assert len({a["attempt_id"] for a in saved}) == 2
    diagnostic = next(e['payload'] for e in records[0]['events'] if e['kind'] == 'diagnostic')
    assert diagnostic['metadata']['prompt'] == 'Only answer candy'


def test_history_window_does_not_delete_evidence(store):
    configure(store)
    for _ in range(12):
        job = prompt_claim(store)
        store.finish_prompt(job, s.run_prompt_round(job, store, FakeAccounts(), Answers("21")))
    assert len(store.public(7)["prompt_test"]["history"]) == 10
    with store.db() as db:
        assert db.execute("SELECT count(*) FROM prompt_results").fetchone()[0] == 12
    assert len(attempts.listing(store, 7)["attempts"]) == 12


def test_cancelled_round_keeps_response_already_received(store):
    configure(store)
    job = prompt_claim(store)
    control = Answers("21")
    def changed(*args, **kwargs):
        store.configure_prompt(7, False, "gpt-6.1-sol", s.DEFAULT_PROMPT)
        return "21"
    control.prompt = changed
    with pytest.raises(s.Cancelled):
        s.run_prompt_round(job, store, FakeAccounts(), control)
    entry = attempts.listing(store, 7)["attempts"][0]
    assert entry['finished']['output_chars'] == 2
    record = attempts.detail(store, 7, entry['attempt_id'])
    assert next(e['payload']['output'] for e in record['events'] if e['kind'] == 'finished') == '21'


def test_failure_keeps_classified_result_without_raw_traffic_or_credentials(store):
    account = FakeAccounts().get(7)
    job = dict(account_id=7, model="gpt-6-astra", generation=1)
    def failed():
        attempts.request_snapshot("https://chatgpt.com/backend-api/codex/responses",
                                  {"Authorization": "Bearer test-secret", "Cookie": "private", "Version": "0.166.0"},
                                  {"model": job["model"]}, "responses")
        attempts.stream_line(b'data: {"delta":"partial test-secret"}')
        raise s.ProbeError("interrupted")
    with pytest.raises(s.ProbeError):
        attempts.invoke(store, job, "run", account, {"kind": "qa"}, failed)
    entry = attempts.listing(store, 7)["attempts"][0]
    detail = attempts.detail(store, 7, entry["attempt_id"])
    encoded = json.dumps(detail)
    assert "test-secret" not in encoded and "Cookie" not in encoded and "Authorization" not in encoded
    assert "partial" not in encoded and 'line_base64' not in encoded
    assert {e['kind'] for e in detail['events']} == {'request', 'finished'}
    assert 'body' not in detail['events'][0]['payload']
    assert entry["finished"]["status"] == "failed"


def test_no_completion_after_process_interruption_is_visible(store):
    with store.db() as db:
        db.execute("INSERT INTO probe_attempts(attempt_id,run_id,account_id,started_at,metadata) VALUES(?,?,?,?,?)",
                   ("interrupted", "run", 7, time.time(), "{}"))
    store.recover()
    assert attempts.listing(store, 7)["attempts"][0]["finished"]["status"] == "unfinished"


def test_evidence_api_auth_account_isolation_and_pagination(store):
    job = dict(account_id=7, model="gpt-6-astra", generation=1)
    for i in range(21):
        attempts.invoke(store, job, "run", FakeAccounts().get(7), {"index": i}, lambda: "21")
    client = s.create_app(store, FakeAccounts(), lambda h: h.get("Authorization") == "ok").test_client()
    assert client.get("/accounts/7/attempts").status_code == 401
    headers = {"Authorization": "ok"}
    page = client.get("/accounts/7/attempts", headers=headers).json
    assert len(page["attempts"]) == 20 and page["next_cursor"]
    second = client.get(f'/accounts/7/attempts?before={page["next_cursor"]}', headers=headers).json
    assert len(second["attempts"]) == 1
    identity = page["attempts"][0]["attempt_id"]
    assert client.get(f"/accounts/8/attempts/{identity}", headers=headers).status_code == 404


def test_bank_cannot_confirm_identity_in_new_conditions(monkeypatch):
    monkeypatch.setattr(s, "analyze_global_outputs", lambda *args: {
        "results": [{"model": "gpt-5.6-luna", "probability": .99999}, {"model": "gpt-6-astra", "probability": .00001}],
        "probability": .99999, "prediction": "gpt-5.6-luna", "used_outputs": 3})
    result = s.summarize([{}, {}, {}], "gpt-6-astra")
    assert result["status"] == "yellow" and not result["confident"]
    assert result["bank_applicability"] == "unverified" and result["candidate_separated"]
    assert result['outcome'] == 'candidate_mismatch'
    assert 'gpt-5.6-luna' in result['message']


@pytest.mark.parametrize('second,score,expected', [('gpt-5.6-luna', .99999, 'red'),
                                                  ('gpt-6-astra', .99999, 'yellow'),
                                                  ('gpt-5.6-luna', .55, 'yellow')])
def test_unverified_candidate_mismatch_gets_independent_confirmation_but_never_auto_pauses(store, monkeypatch, second, score, expected):
    from test_service import FakeControl
    store.configure(7, True, 'gpt-6-astra', True, 1, challenge_count=1)
    scored = []
    def analyze(outputs, bank):
        prediction = 'gpt-5.6-luna' if not scored else second
        probability = .99999 if not scored else score
        scored.append(outputs)
        return {'results': [{'model':prediction,'probability':probability},
                            {'model':'other','probability':1-probability}],
                'prediction':prediction,'probability':probability,'used_outputs':1}
    monkeypatch.setattr(s, 'analyze_global_outputs', analyze)
    prompts = []
    def probe(account, model, challenge, *args):
        prompts.append(challenge['prompt'])
        return {'text':f'sample {len(prompts)}'}, model
    job = claim(store)
    result = s.run_round(job, store, FakeAccounts(), '', probe)
    assert result['status'] == expected and result['requests'] == result['valid_samples'] == 2
    assert len(result['stages']) == len(result['confirmation']) == 2
    assert len(set(prompts)) == 2 and scored[0] != scored[1]
    assert result['budget_seconds'] == 1200
    assert not result['confident'] and result['bank_applicability'] == 'unverified'
    store.finish(job, result)
    controller = FakeControl()
    store.apply_pending_pause(FakeAccounts(), controller)
    assert controller.calls == [] and store.get(7)['mismatch_streak'] == 0


def test_large_evidence_is_bounded_on_screen_and_complete_on_download(store):
    payload = json.dumps({'line':'event ' + 'x' * 430, 'line_base64':'unchanged-by-export'})
    big_output = 'answer' * 40000
    with store.db() as db:
        db.execute('INSERT INTO probe_attempts(attempt_id,run_id,account_id,started_at,metadata) VALUES(?,?,?,?,?)',
                   ('large', 'run', 7, time.time(), json.dumps({'kind':'qa','prompt':'p' * 9000})))
        db.executemany('INSERT INTO probe_attempt_events(attempt_id,at,kind,payload) VALUES(?,?,?,?)',
                       [('large', time.time(), 'stream', payload)] * 15254)
        db.execute('INSERT INTO probe_attempt_events(attempt_id,at,kind,payload) VALUES(?,?,?,?)',
                   ('large',time.time(),'finished',json.dumps({'status':'completed','output':big_output})))
    client = s.create_app(store, FakeAccounts(), lambda h: h.get('Authorization') == 'ok').test_client()
    headers = {'Authorization':'ok'}
    base = '/accounts/7/attempts/large'
    assert client.get(base+'/events').status_code == client.get(base+'/download').status_code == 401
    listing = client.get('/accounts/7/attempts', headers=headers)
    assert len(listing.data) < 1500 and 'prompt' not in listing.json['attempts'][0]['metadata']
    assert listing.json['attempts'][0]['finished']['output_chars'] == len(big_output)
    key_page = client.get(base+'/events', headers=headers).json
    assert key_page['event_count'] == 1 and key_page['total_event_count'] == 15255
    assert key_page['events'][0]['kind'] == 'finished'
    assert client.get(base+'/events?view=invalid', headers=headers).status_code == 400
    page = client.get(base+'/events?view=all', headers=headers)
    assert len(page.json['events']) == 40 and page.json['event_count'] == 15255 and len(page.data) < 90000
    last = page.json['events'][-1]['sequence']
    second_page = client.get(base+f'/events?view=all&after={page.json["next_cursor"]}', headers=headers).json
    assert second_page['events'][0]['sequence'] > last
    tail = client.get(base+'/events?after=15254', headers=headers).json
    assert len(tail['events']) == 1 and tail['events'][0]['truncated'] and len(tail['events'][0]['preview']) == 2000
    assert client.get('/accounts/8/attempts/large/events', headers=headers).status_code == 404
    assert client.get('/accounts/8/attempts/large/download', headers=headers).status_code == 404
    downloaded = client.get(base+'/download', headers=headers)
    assert downloaded.json == attempts.detail(store, 7, 'large')
    assert downloaded.json['events'][-1]['payload']['output'] == big_output
    assert 'attachment' in downloaded.headers['Content-Disposition']


def test_attribution_analysis_survives_round_cancellation(store, monkeypatch):
    store.configure(7, True, 'gpt-6-astra', challenge_count=1)
    job = claim(store)
    def score(outputs, model):
        store.configure(7, False, model)
        return {"status": "yellow", "prediction": "gpt-5.6-luna", "probability": .95,
                "confident": False, "analysis": {"all_candidates": ["candidate-a", "candidate-b"]}}
    monkeypatch.setattr(s, "summarize", score)
    with pytest.raises(s.Cancelled):
        s.run_round(job, store, FakeAccounts(), "0.166.0", lambda *args: ({"text": "full numeric output"}, job["model"]))
    entry = attempts.listing(store, 7)["attempts"][0]
    events = attempts.detail(store, 7, entry["attempt_id"])["events"]
    analysis = next(e["payload"] for e in events if e["kind"] == "analysis")
    assert analysis["analysis"]["all_candidates"] == ["candidate-a", "candidate-b"]
    assert analysis["attempt_ids"] == [entry["attempt_id"]]


def test_http_error_body_is_neither_read_nor_persisted(store):
    class Response:
        def iter_content(self, chunk_size):
            pytest.fail('raw error body must not be collected')
    job = dict(account_id=7, model="gpt-6-astra", generation=1)
    attempts.invoke(store, job, "error", FakeAccounts().get(7), {},
                    lambda: attempts.error_body(Response(), 80))
    entry = attempts.listing(store, 7)["attempts"][0]
    events = attempts.detail(store, 7, entry["attempt_id"])["events"]
    assert [e['kind'] for e in events] == ['finished']


def test_stream_size_does_not_grow_results_database(store):
    job = dict(account_id=7, model='gpt-6-astra', generation=1)
    def send():
        attempts.request_snapshot('https://example.test/responses', {'X-Client-Request-Id':'linked'},
                                  {'model':'gpt-6-astra','instructions':'RAW-INSTRUCTIONS','input':'RAW-BODY'}, 'responses')
        attempts.emit('gateway_request', {'client_request_id':'linked', 'upstream_attempt_id':'upstream-linked',
            'details':{'account_id':7,'transport':'codex-http1-openssl36','body':{'input':'RAW-BODY'}}})
        attempts.compare_contract({'instructions':'RAW-CONTRACT'})
        for _ in range(15254):
            attempts.stream_line(b'data: RAW-STREAM' + b'x' * 430)
        attempts.emit('parsed_output', {'output':'DUPLICATE-OUTPUT'})
        return '29'
    _, identity = attempts.invoke(store, job, 'results-only', FakeAccounts().get(7), {'kind':'qa'}, send)
    data = attempts.detail(store, 7, identity)
    assert data['metadata']['capture_mode'] == 'first-normal-anomaly-v1'
    encoded = json.dumps(data)
    assert all(marker not in encoded for marker in ('RAW-INSTRUCTIONS','RAW-BODY','RAW-CONTRACT','RAW-STREAM','DUPLICATE-OUTPUT','line_base64'))
    assert 'upstream-linked' in encoded and data['events'][-1]['payload']['output'] == '29'
    assert len(data['events']) == 4 and len(encoded) < 3000
    with store.db() as db:
        assert db.execute('SELECT count(*) FROM probe_attempt_events WHERE kind IN (\'stream\',\'error_body\',\'parsed_output\')').fetchone()[0] == 0


def test_insufficient_sample_preserves_the_answer_once_without_stream(store):
    job = dict(account_id=7, model='gpt-6-astra', generation=1)
    def insufficient():
        attempts.emit('parsed_output', {'output':'137 42', 'parsed_count':2, 'minimum_count':80})
        raise s.InsufficientSample('too few numbers')
    with pytest.raises(s.InsufficientSample):
        attempts.invoke(store, job, 'insufficient', FakeAccounts().get(7), {}, insufficient)
    entry = attempts.listing(store, 7)['attempts'][0]
    data = attempts.detail(store, 7, entry['attempt_id'])
    assert len(data['events']) == 1 and data['events'][0]['payload']['output'] == '137 42'
    assert data['events'][0]['payload']['parsed_count'] == 2


@pytest.mark.parametrize("change", ["context", "metadata", "proxy", "instructions", "effort", "transport"])
def test_comparison_rejects_changed_conditions_and_keeps_evidence(store, change):
    import copy
    job = dict(account_id=7, model="gpt-6-astra", generation=1)
    request = {"profile": s.COMPARISON_PROFILE, "details": {
        "account_id": 7, "proxy_id": 3, "url": "https://chatgpt.com/backend-api/codex/responses",
        "transport": "codex-http1-openssl36", "body": {
            "model": job["model"], "reasoning": {"effort": "xhigh"}, "instructions": s.COMPARISON_INSTRUCTIONS,
            "tools": [], "tool_choice": "none", "parallel_tool_calls": False, "text": {"verbosity": "medium"},
            "input": [{"role": "developer", "content": "fixed context"},
                      {"role": "user", "content": [{"type": "input_text", "text": "question"}]}],
            "client_metadata": {"turn_id": "first", "capability": "original"}}}}
    def send(data):
        data["client_request_id"] = attempts.ACTIVE.get()["id"]
        s.validate_comparison_request(data, data["client_request_id"], 7, job["model"], "question")
        return "answer"
    attempts.invoke(store, job, "contract", FakeAccounts().get(7), {}, lambda: send(request))
    fresh = copy.deepcopy(request)
    fresh["details"]["body"]["client_metadata"]["turn_id"] = "second"
    attempts.invoke(store, job, "contract", FakeAccounts().get(7), {}, lambda: send(fresh))
    body = fresh["details"]["body"]
    if change == "context": body["input"][0]["content"] = "changed"
    if change == "metadata": body["client_metadata"]["capability"] = "changed"
    if change == "proxy": fresh["details"]["proxy_id"] = 8
    if change == "instructions": body["instructions"] = "changed"
    if change == "effort": body["reasoning"] = {"effort": "low"}
    if change == "transport": fresh["details"]["transport"] = "python-requests"
    with pytest.raises(s.ProbeError):
        attempts.invoke(store, job, "contract", FakeAccounts().get(7), {}, lambda: send(fresh))
    saved = attempts.listing(store, 7)["attempts"]
    assert len(saved) == 3 and saved[0]["finished"]["status"] == "failed"


@pytest.mark.parametrize("prompt", [False, True])
def test_legacy_pending_attribution_never_pauses_without_valid_bank(store, prompt):
    from test_service import FakeControl
    table = "prompt_monitors" if prompt else "monitors"
    streak = "failure_streak" if prompt else "mismatch_streak"
    context = "failure_context" if prompt else "mismatch_context"
    if prompt:
        store.configure_prompt(7, True, "gpt-6-sol", "{{challenge}}", True, 1)
    else:
        store.configure(7, True, "gpt-6-sol", True, 1)
    with store.db() as db:
        db.execute(f"UPDATE {table} SET {streak}=1,{context}=?,pause_status='pending'",
                   (s.account_context(FakeAccounts().get(7)).hex(),))
    control = FakeControl()
    if prompt: store.apply_pending_prompt_pause(FakeAccounts(), control)
    else: store.apply_pending_pause(FakeAccounts(), control)
    assert control.calls == []
    with store.db() as db:
        assert db.execute(f"SELECT {streak},pause_status FROM {table}").fetchone()[:] == (0, "")
