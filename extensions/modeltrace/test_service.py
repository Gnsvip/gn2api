import copy
import json
import time

import pytest

import service as s


def account():
    return dict(id=7, platform='openai', type='oauth', status='active', schedulable=True,
                parent_account_id=None, credentials={'access_token': 'test-secret', 'model_mapping': {}}, extra={})


class FakeAccounts:
    def __init__(self):
        self.account = account()
        self.reads = []

    def metadata(self, ids):
        return {i: dict(id=i, platform='openai', type='oauth', status='active', schedulable=True, mapping={}) for i in ids}

    def existing_ids(self, ids):
        return set(ids)

    def get(self, id):
        self.reads.append(id)
        return copy.deepcopy(self.account)


@pytest.fixture
def store(tmp_path):
    return s.Store(tmp_path / 'state.sqlite3')


def claim(store):
    with store.db() as db:
        db.execute('UPDATE monitors SET next_run=0')
    return store.claim()


def test_default_disabled_no_jobs_or_history(store):
    assert not store.public(7)['enabled']
    assert store.public(7)['history'] == []
    assert store.claim() is None


def test_per_account_persistence_and_ten_history_order(store):
    store.configure(7, True, 'gpt-6-sol')
    for i in range(12):
        job = claim(store)
        store.finish(job, {'status': 'green', 'message': str(i)})
    fresh = s.Store(store.path)
    assert fresh.public(7)['enabled'] is True
    assert [r['message'] for r in fresh.public(7)['history']] == [str(i) for i in range(2, 12)]
    assert not fresh.public(8)['enabled']
    assert fresh.claim() is None  # next run is ten minutes away


def test_disable_discards_running_result_and_reenable_generation(store):
    store.configure(7, True, 'gpt-6-sol')
    old_job = claim(store)
    store.configure(7, False, 'gpt-6-sol')
    store.configure(7, True, 'gpt-6-sol')
    store.finish(old_job, {'status': 'green'})
    assert store.public(7)['history'] == []
    assert not store.is_current(old_job)


def test_repeated_enable_does_not_reset_schedule(store):
    store.configure(7, True, 'gpt-6-sol')
    before = store.get(7)
    store.configure(7, True, 'gpt-6-sol')
    assert store.get(7) == before


def test_admin_auth_origin_unknown_models(store):
    app = s.create_app(store, FakeAccounts(), lambda h: h.get('Authorization') == 'Bearer admin')
    client = app.test_client()
    assert client.get('/accounts?ids=7').status_code == 401
    assert client.get('/accounts?ids=7', headers={'Authorization': 'Bearer user'}).status_code == 401
    headers = {'Authorization': 'Bearer admin'}
    assert client.get('/accounts?ids=7', headers=headers).json['accounts']['7']['enabled'] is False
    assert client.put('/accounts/7', json={'enabled': True, 'model': 'gpt-6.1-sol'}, headers=headers).status_code == 400
    assert client.put('/accounts/7', json={'enabled': 'false'}, headers=headers).status_code == 400
    assert client.put('/accounts/7', json={'enabled': True, 'model': 'gpt-6-sol'}, headers={**headers, 'Origin': 'https://evil.example'}).status_code == 403
    assert client.put('/accounts/7', json={'enabled': True, 'model': 'gpt-6-sol'}, headers=headers).status_code == 200
    assert client.get('/accounts?ids=7,8', headers=headers).json['accounts']['8']['enabled'] is False
    assert 'test-secret' not in client.get('/accounts?ids=7', headers=headers).text


class Response:
    def __init__(self, events):
        self.events = events

    def iter_lines(self, **kwargs):
        yield from [b'data: ' + (e.encode() if isinstance(e, str) else json.dumps(e).encode()) for e in self.events]


@pytest.mark.parametrize('protocol,events', [
    ('responses', [{'type': 'response.output_text.delta', 'delta': '123'}, {'type': 'response.completed', 'response': {'status': 'completed'}}]),
    ('anthropic', [{'type': 'content_block_delta', 'delta': {'type': 'text_delta', 'text': '123'}}, {'type': 'message_stop'}]),
    ('chat', [{'choices': [{'delta': {'content': '123'}, 'finish_reason': 'stop'}]}, '[DONE]']),
])
def test_three_stream_formats(protocol, events):
    assert s.read_stream(Response(events), protocol, lambda: False, time.monotonic() + 10) == '123'


def test_http_200_stream_errors_and_incomplete_are_not_green():
    for events in ([{'type': 'response.output_text.delta', 'delta': '123'}],
                   [{'type': 'response.output_text.delta', 'delta': '123'}, {'type': 'response.failed'}],
                   [{'type': 'response.completed', 'response': {'status': 'incomplete'}}]):
        with pytest.raises(s.ProbeError):
            s.read_stream(Response(events), 'responses', lambda: False, time.monotonic() + 10)


@pytest.mark.parametrize('event', [
    {'type': 'error', 'code': 'server_is_overloaded', 'message': 'upstream-secret'},
    {'type': 'response.failed', 'response': {'error': {'code': 'server_is_overloaded', 'message': 'upstream-secret'}}},
])
def test_stream_overload_is_identified_without_leaking_upstream_text(event):
    with pytest.raises(s.ProbeError) as exc:
        s.read_stream(Response([event]), 'responses', lambda: False, time.monotonic() + 10)
    assert 'server_is_overloaded' in str(exc.value) and '繁忙' in str(exc.value)
    assert 'upstream-secret' not in str(exc.value)


def test_unknown_stream_error_never_echoes_arbitrary_code_or_message():
    event = {'type': 'error', 'code': 'credential-secret', 'message': 'raw-private-message'}
    assert s.stream_failure_message(event) == '上游流内报错，本轮未完成'


@pytest.mark.parametrize('count,accepted', [(164, False), (165, True)])
def test_probe_sample_floor_and_diagnostic_count(monkeypatch, count, accepted):
    text = ','.join(['137'] * count)
    class StreamResponse(Response):
        status_code = 200
        headers = {'Content-Type': 'text/event-stream'}
        def __enter__(self): return self
        def __exit__(self, *args): pass
    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def post(self, *args, **kwargs):
            return StreamResponse([{'type': 'response.output_text.delta', 'delta': text},
                {'type': 'response.completed', 'response': {'status': 'completed'}}])
    monkeypatch.setattr(s.requests, 'Session', Session)
    args = (account(), 'gpt-6-astra', {'prompt': 'fixture challenge', 'expected_count': 300},
            lambda: False, time.monotonic() + 30, '0.163.0')
    if accepted:
        output, _ = s.probe(*args)
        assert len(s.parse_numbers(output['text'])) == count
    else:
        with pytest.raises(s.InsufficientSample) as exc:
            s.probe(*args)
        assert '识别 164 个' in str(exc.value) and '至少需要 165 个' in str(exc.value)
        assert '本次要求 300 个' in str(exc.value)


@pytest.mark.parametrize('delay,remaining,expected_timeout,success', [
    (56.7, 240, 120, True),
    (121, 240, 120, False),
    (56.7, 30, 30, False),
])
def test_probe_waits_for_buffered_headers_but_honors_remaining_budget(monkeypatch, delay, remaining, expected_timeout, success):
    clock, calls = [1000.0], []
    monkeypatch.setattr(s.time, 'monotonic', lambda: clock[0])
    class StreamResponse(Response):
        status_code = 200
        headers = {'Content-Type': 'text/event-stream'}
        def __enter__(self): return self
        def __exit__(self, *args): pass
    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def post(self, *args, **kwargs):
            calls.append(kwargs['timeout'])
            wait = kwargs['timeout'][1]
            clock[0] += min(delay, wait)
            if delay > wait:
                raise s.requests.ReadTimeout('private-url-and-secret')
            return StreamResponse([
                {'type': 'response.output_text.delta', 'delta': ','.join(['137'] * 300)},
                {'type': 'response.completed', 'response': {'status': 'completed'}}])
    monkeypatch.setattr(s.requests, 'Session', Session)
    args = (account(), 'gpt-6-astra', {'prompt': 'fixture', 'expected_count': 300},
            lambda: False, clock[0] + remaining, '0.163.0')
    if success:
        output, _ = s.probe(*args)
        assert len(s.parse_numbers(output['text'])) == 300
    else:
        with pytest.raises(s.ProbeError) as exc:
            s.probe(*args)
        assert '超时' in str(exc.value) and 'private' not in str(exc.value)
        assert isinstance(exc.value, s.RoundTimeout) == (remaining < 120)
    assert calls == [(8, expected_timeout)]  # A timeout must not retry the paid request.


@pytest.mark.parametrize('error,expected', [
    (s.requests.ConnectTimeout('private-url-and-secret'), '连接上游超时'),
    (s.requests.ReadTimeout('private-url-and-secret'), '连续 120 秒未收到数据'),
    (s.requests.ConnectionError(s.ReadTimeoutError(None, 'private-url', 'private-secret')), '连续 120 秒未收到数据'),
    (s.requests.ConnectionError('private-url-and-secret'), '上游连接失败或连接中断'),
    (s.requests.exceptions.SSLError('private-url-and-secret'), 'TLS 连接失败'),
])
def test_transport_errors_identify_read_timeout_without_exposing_raw_details(error, expected):
    message = s.transport_failure_message(error, 120)
    assert expected in message and 'private' not in message


def test_streaming_timeout_after_http_200_stays_yellow_and_never_pauses(store, monkeypatch):
    store.configure(7, True, 'gpt-6-astra', True, 1)
    class StreamResponse:
        status_code = 200
        headers = {'Content-Type': 'text/event-stream'}
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def iter_lines(self, **kwargs):
            yield b'data: {"type":"response.created"}'
            raise s.requests.ConnectionError(s.ReadTimeoutError(None, 'private-url', 'secret'))
    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def post(self, *args, **kwargs): return StreamResponse()
    monkeypatch.setattr(s.requests, 'Session', Session)
    job = claim(store)
    result = s.run_round(job, store, FakeAccounts(), '0.163.0')
    assert result['requests'] == 1 and result['valid_samples'] == 0
    assert result['status'] == 'yellow' and '连续 120 秒' in result['message']
    store.finish(job, result)
    assert store.get(7)['mismatch_streak'] == 0 and store.get(7)['pause_status'] == ''


def test_late_stream_completion_is_rejected_after_round_deadline(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(s.time, 'monotonic', lambda: clock[0])
    class SlowResponse:
        def iter_lines(self, **kwargs):
            yield b'data: {"type":"response.output_text.delta","delta":"137"}'
            clock[0] += 241
            yield b'data: {"type":"response.completed","response":{"status":"completed"}}'
    with pytest.raises(s.ProbeError, match='超时'):
        s.read_stream(SlowResponse(), 'responses', lambda: False, 1240)


def test_oauth_uses_challenge_model_mapping_and_current_account():
    a = account()
    a['credentials']['chatgpt_account_id'] = 'expected-account'
    a['credentials']['model_mapping'] = {'gpt-6-sol': 'mapped-model'}
    url, headers, payload, protocol, model = s.build_request(a, 'gpt-6-sol', 'real challenge', '0.163.0')
    assert url == 'https://chatgpt.com/backend-api/codex/responses'
    assert payload['input'][0]['content'][0]['text'] == 'real challenge'
    assert payload['model'] == 'mapped-model'
    assert headers['ChatGPT-Account-Id'] == 'expected-account'
    assert 'max_output_tokens' not in payload
    assert payload['store'] is False


def test_apikey_protocol_override_and_proxy_no_fallback():
    a = account()
    a['type'] = 'apikey'
    a['credentials'] = {'api_key': 'secret', 'base_url': 'https://upstream.example/v1'}
    a['extra'] = {'openai_responses_mode': 'force_chat_completions'}
    url, _, payload, protocol, _ = s.build_request(a, 'gpt-6-sol', 'challenge', '0.163.0')
    assert url == 'https://upstream.example/v1/chat/completions'
    assert protocol == 'chat'
    a['proxy_id'] = 5
    with pytest.raises(s.ProbeError):
        s.proxy_url(a)


def test_round_stays_on_one_account_and_uses_one_batch(store, monkeypatch):
    store.configure(7, True, 'gpt-6-sol')
    job = claim(store)
    accounts, calls = FakeAccounts(), []
    def fake_probe(a, model, challenge, *args):
        calls.append((a['id'], model))
        return {'text': 'x', 'expected_count': 300}, model
    monkeypatch.setattr(s, 'summarize', lambda outputs, model: dict(status='yellow', confident=True, prediction='gpt-5.4', probability=.99))
    result = s.run_round(job, store, accounts, '0.163.0', fake_probe)
    assert result['status'] == 'red'
    assert len(calls) == 3
    assert set(calls) == {(7, 'gpt-6-sol')}
    assert set(accounts.reads) == {7}
    assert result['valid_samples'] == 3 and result['insufficient_samples'] == 0


@pytest.mark.parametrize('failed_calls', [set(), {2}, {1, 3}, {1, 3, 5}])
def test_insufficient_sample_never_adds_replacement_requests(store, monkeypatch, failed_calls):
    store.configure(7, True, 'gpt-6-sol')
    calls, scored, deadlines = [], [], []
    def fake_probe(a, model, challenge, cancelled, deadline, version):
        calls.append(challenge)
        deadlines.append(deadline)
        if len(calls) in failed_calls:
            raise s.InsufficientSample('有效数字样本不足：识别 10 个，至少需要 165 个')
        return {'text': str(len(calls)), 'expected_count': 300}, model
    def summarize(outputs, model):
        scored.append(outputs)
        return dict(status='green', confident=True, prediction=model, probability=.99)
    monkeypatch.setattr(s, 'summarize', summarize)
    result = s.run_round(claim(store), store, FakeAccounts(), '0.163.0', fake_probe)
    assert result['status'] == ('yellow' if failed_calls else 'green')
    assert result['requests'] == len(calls) == 3
    failed = len(failed_calls & {1, 2, 3})
    assert result['valid_samples'] == 3 - failed and result['insufficient_samples'] == failed
    assert [[o['text'] for o in outputs] for outputs in scored] == ([] if failed else [['1', '2', '3']])
    assert len({id(c) for c in calls}) == len(calls)  # Each request uses a fresh challenge.
    assert len(set(deadlines)) == 1
    assert max(deadlines) - min(deadlines) == 0


def test_one_batch_without_enough_samples_stay_yellow_and_never_pause(store, monkeypatch):
    store.configure(7, True, 'gpt-6-sol', True, 1)
    def fake_probe(a, model, *args):
        raise s.InsufficientSample('有效数字样本不足：识别 10 个，至少需要 165 个')
    monkeypatch.setattr(s, 'summarize', lambda *args: pytest.fail('Incomplete samples must not be scored'))
    job = claim(store)
    result = s.run_round(job, store, FakeAccounts(), '0.163.0', fake_probe)
    assert result['status'] == 'yellow' and result['requests'] == 3
    assert result['valid_samples'] == 0 and result['insufficient_samples'] == 3
    assert '0/3' in result['message'] and '识别 10 个' in result['message']
    store.finish(job, result)
    assert store.get(7)['mismatch_streak'] == 0 and store.get(7)['pause_status'] == ''


def test_partial_batch_stays_yellow_without_replacement(store, monkeypatch):
    store.configure(7, True, 'gpt-6-sol', True, 1)
    calls = []
    def fake_probe(a, model, challenge, *args):
        calls.append(challenge)
        if len(calls) == 2:
            raise s.InsufficientSample('有效数字样本不足')
        return {'text': str(len(calls)), 'expected_count': 300}, model
    monkeypatch.setattr(s, 'summarize', lambda outputs, model: dict(
        status='yellow', confident=True, prediction='gpt-5.4', probability=.99))
    job = claim(store)
    result = s.run_round(job, store, FakeAccounts(), '0.163.0', fake_probe)
    assert result['status'] == 'yellow' and result['requests'] == len(calls) == 3
    assert result['valid_samples'] == 2 and result['insufficient_samples'] == 1
    assert '不自动补采' in result['message'] and 'confirmation' not in result
    store.finish(job, result)
    assert store.get(7)['mismatch_streak'] == 0 and store.get(7)['pause_status'] == ''


def test_mismatch_does_not_depend_on_any_followup_response(store, monkeypatch):
    store.configure(7, True, 'gpt-6-sol', True, 1)
    calls, scored = [], []
    def fake_probe(a, model, challenge, *args):
        calls.append(challenge)
        if len(calls) > 3:
            pytest.fail('No followup request is allowed')
        return {'text': str(len(calls)), 'expected_count': 300}, model
    def summarize(outputs, model):
        scored.append(outputs)
        return dict(status='yellow', confident=True, prediction='gpt-5.4', probability=.99)
    monkeypatch.setattr(s, 'summarize', summarize)
    job = claim(store)
    result = s.run_round(job, store, FakeAccounts(), '0.163.0', fake_probe)
    assert result['status'] == 'red' and result['requests'] == len(calls) == 3
    assert len(scored) == 1 and result['valid_samples'] == 3
    assert 'confirmation' not in result
    store.finish(job, result)
    assert store.get(7)['pause_status'] == 'pending'


def test_other_probe_errors_are_not_retried_even_after_sample_replacement(store):
    store.configure(7, True, 'gpt-6-sol')
    calls = []
    def fake_probe(*args):
        calls.append(1)
        if len(calls) == 1:
            raise s.InsufficientSample('有效数字样本不足')
        raise s.ProbeError('上游 HTTP 429，本轮停止')
    result = s.run_round(claim(store), store, FakeAccounts(), '0.163.0', fake_probe)
    assert result['status'] == 'yellow' and result['requests'] == len(calls) == 2
    assert 'HTTP 429' in result['message'] and result['insufficient_samples'] == 1


@pytest.mark.parametrize('change', ['disable', 'unschedulable', 'route', 'deadline'])
def test_replacement_rechecks_account_cancellation_and_deadline(store, monkeypatch, change):
    store.configure(7, True, 'gpt-6-sol')
    accounts, calls = FakeAccounts(), []
    monotonic = time.monotonic
    def fake_probe(a, model, *args):
        calls.append(1)
        if change == 'disable':
            store.configure(7, False, model)
        elif change == 'unschedulable':
            accounts.account['schedulable'] = False
        elif change == 'route':
            accounts.account['credentials']['model_mapping'] = {model: 'gpt-5.4'}
        else:
            monkeypatch.setattr(s.time, 'monotonic', lambda: monotonic() + 720)
        raise s.InsufficientSample('有效数字样本不足')
    job = claim(store)
    if change == 'disable':
        with pytest.raises(s.Cancelled):
            s.run_round(job, store, accounts, '0.163.0', fake_probe)
    else:
        result = s.run_round(job, store, accounts, '0.163.0', fake_probe)
        assert result['status'] == 'yellow' and result['requests'] == 1
        assert {'unschedulable': '不可调度', 'route': '配置已变化', 'deadline': '超时'}[change] in result['message']
    assert len(calls) == 1


def test_partial_samples_are_never_reused_across_rounds(store, monkeypatch):
    store.configure(7, True, 'gpt-6-sol')
    calls = []
    def fake_probe(a, model, *args):
        calls.append(1)
        if len(calls) % 3 in (1, 2):
            return {'text': 'partial-round-success', 'expected_count': 300}, model
        raise s.InsufficientSample('有效数字样本不足')
    monkeypatch.setattr(s, 'summarize', lambda *args: pytest.fail('No round has three valid responses'))
    for _ in range(2):
        job = claim(store)
        result = s.run_round(job, store, FakeAccounts(), '0.163.0', fake_probe)
        assert result['status'] == 'yellow' and result['requests'] == 3 and result['valid_samples'] == 2
        store.finish(job, result)
    assert len(calls) == 6


def test_disable_between_probes_prevents_next_request(store):
    store.configure(7, True, 'gpt-6-sol')
    calls = []
    def fake_probe(a, model, challenge, *args):
        calls.append(a['id'])
        store.configure(7, False, model)
        return {'text': 'x', 'expected_count': 300}, model
    with pytest.raises(s.Cancelled):
        s.run_round(claim(store), store, FakeAccounts(), '0.163.0', fake_probe)
    assert calls == [7]


def test_recent_business_requests_do_not_starve_detection(store, monkeypatch):
    store.configure(7, True, 'gpt-6-sol')
    accounts = FakeAccounts()
    accounts.account['last_used_at'] = time.time()
    calls = []
    def fake_probe(a, model, challenge, *args):
        calls.append(a['id'])
        # Simulate normal traffic continuing throughout the three challenges.
        accounts.account['last_used_at'] = time.time()
        return {'text': 'x', 'expected_count': 300}, model
    monkeypatch.setattr(s, 'summarize', lambda outputs, model: dict(
        status='green', confident=True, prediction=model, probability=.99))
    result = s.run_round(claim(store), store, accounts, '0.163.0', fake_probe)
    assert calls == [7, 7, 7]
    assert result['status'] == 'green' and result['requests'] == 3


@pytest.mark.parametrize('field,percent', [('codex_5h_used_percent', 95), ('codex_7d_used_percent', 100)])
def test_usage_window_percent_does_not_block_credit_backed_requests(store, monkeypatch, field, percent):
    store.configure(7, True, 'gpt-6-sol')
    accounts = FakeAccounts()
    accounts.account['extra'][field] = percent
    calls = []
    def fake_probe(a, model, challenge, *args):
        calls.append(a['id'])
        return {'text': 'x', 'expected_count': 300}, model
    monkeypatch.setattr(s, 'summarize', lambda outputs, model: dict(
        status='green', confident=True, prediction=model, probability=.99))
    result = s.run_round(claim(store), store, accounts, '0.163.0', fake_probe)
    assert calls == [7, 7, 7]
    assert result['requests'] == 3 and result['status'] == 'green'


def test_real_cooldown_still_skips_and_upstream_rejection_stays_yellow(store):
    store.configure(7, True, 'gpt-6-sol')
    accounts = FakeAccounts()
    accounts.account['rate_limit_reset_at'] = time.time() + 3600
    job = claim(store)
    result = s.run_round(job, store, accounts, '0.163.0', lambda *args: pytest.fail('no call expected'))
    assert result['requests'] == 0 and result['status'] == 'yellow'
    accounts.account['rate_limit_reset_at'] = None
    def reject(*args):
        raise s.ProbeError('上游 HTTP 429')
    result = s.run_round(job, store, accounts, '0.163.0', reject)
    assert result['requests'] == 1 and result['status'] == 'yellow'


def test_changed_routing_invalidates_round(store):
    store.configure(7, True, 'gpt-6-sol')
    accounts = FakeAccounts()
    calls = []
    def fake_probe(a, model, challenge, *args):
        calls.append(a['id'])
        accounts.account['credentials']['model_mapping'] = {'gpt-6-sol': 'gpt-5.4'}
        return {'text': 'x', 'expected_count': 300}, model
    result = s.run_round(claim(store), store, accounts, '0.163.0', fake_probe)
    assert result['status'] == 'yellow'
    assert calls == [7]


@pytest.mark.parametrize('count', [1, 2, 3])
def test_real_vendor_scoring_accepts_selected_count(count):
    outputs = [{'text': ','.join(str((i * 19 + j * 7) % 355 + 1) for i in range(310)), 'expected_count': 310} for j in range(count)]
    result = s.summarize(outputs, 'gpt-6-sol')
    assert result['samples'] == count
    assert len(result['candidates']) == 3
    assert result['status'] in ('green', 'yellow', 'red')
    assert s.analyze_global_outputs(outputs, s.BANK)['calibration']['queries'] == str(count)


class FakeControl:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def pause(self, account_id):
        self.calls.append(account_id)
        if self.fail:
            raise RuntimeError('must-not-leak-secret')


def finish_round(store, status='red', context=None):
    job = claim(store)
    store.finish(job, {'status': status, 'bank_applicability': 'validated', '_context': context or s.account_context(account()).hex()})
    return job


def test_old_schema_migration_preserves_config_and_starts_disarmed(tmp_path):
    import sqlite3
    path = tmp_path / 'old.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('''CREATE TABLE monitors (account_id INTEGER PRIMARY KEY,enabled INTEGER,
            model TEXT,generation INTEGER,next_run REAL,running INTEGER,updated_at REAL,message TEXT)''')
        db.execute("INSERT INTO monitors VALUES (7,1,'gpt-6-sol',4,12345,0,1,'existing')")
    migrated = s.Store(path)
    state = migrated.public(7)
    assert state['enabled'] and state['model'] == 'gpt-6-sol'
    assert not state['auto_pause_enabled'] and state['auto_pause_threshold'] == 3
    assert state['interval_minutes'] == 10 and state['interval'] == 600
    assert state['challenge_count'] == 3
    assert state['mismatch_streak'] == 0 and state['next_run'] == 12345
    assert s.Store(path).get(7) == migrated.get(7)


def test_automatic_pause_default_off_and_three_red_trigger_only_target(store):
    controller = FakeControl()
    store.configure(7, True, 'gpt-6-sol')
    for _ in range(4):
        finish_round(store)
        store.apply_pending_pause(FakeAccounts(), controller)
    assert controller.calls == [] and store.public(7)['mismatch_streak'] == 0
    store.configure(7, True, 'gpt-6-sol', True, 3)
    for expected in (1, 2):
        finish_round(store)
        store.apply_pending_pause(FakeAccounts(), controller)
        assert store.public(7)['mismatch_streak'] == expected
        assert controller.calls == []
    last = finish_round(store)
    assert store.claim() is None
    store.finish(last, {'status': 'red', 'bank_applicability': 'validated', '_context': s.account_context(account()).hex()})
    store.apply_pending_pause(FakeAccounts(), controller)
    state = store.public(7)
    assert controller.calls == [7]
    assert state['pause_status'] == 'paused' and state['paused_at'] > 0
    assert state['mismatch_streak'] == 3 and not state['enabled']
    assert state['auto_pause_enabled'] and state['history'][-1]['status'] == 'red'
    store.apply_pending_pause(FakeAccounts(), controller)
    assert controller.calls == [7] and store.get(8) is None
    assert '_context' not in json.dumps(state) and 'test-secret' not in json.dumps(state)


@pytest.mark.parametrize('status', ['green', 'yellow'])
def test_non_red_breaks_streak(store, status):
    store.configure(7, True, 'gpt-6-sol', True, 3)
    finish_round(store)
    finish_round(store)
    finish_round(store, status)
    assert store.public(7)['mismatch_streak'] == 0
    finish_round(store)
    assert store.public(7)['mismatch_streak'] == 1
    control = FakeControl()
    store.apply_pending_pause(FakeAccounts(), control)
    assert control.calls == []


@pytest.mark.parametrize('change', [
    (True, 'gpt-6-sol', False, 3), (False, 'gpt-6-sol', True, 3),
    (True, 'gpt-5.4', True, 3), (True, 'gpt-6-sol', True, 4),
])
def test_config_change_cancels_pending_pause_and_resets_count(store, change):
    store.configure(7, True, 'gpt-6-sol', True, 3)
    for _ in range(3):
        last = finish_round(store)
    store.configure(7, *change)
    store.finish(last, {'status': 'red', 'bank_applicability': 'validated', '_context': s.account_context(account()).hex()})
    control = FakeControl()
    store.apply_pending_pause(FakeAccounts(), control)
    assert control.calls == [] and store.public(7)['mismatch_streak'] == 0


def test_streak_survives_restart_and_history_trimming(store):
    store.configure(7, True, 'gpt-6-sol', True, 12)
    for _ in range(11):
        finish_round(store)
    restarted = s.Store(store.path)
    restarted.recover()
    assert restarted.public(7)['mismatch_streak'] == 11
    assert len(restarted.public(7)['history']) == 10
    finish_round(restarted)
    control = FakeControl()
    s.Store(store.path).apply_pending_pause(FakeAccounts(), control)
    assert control.calls == [7]


def test_failed_pause_retries_without_false_success_and_can_be_cancelled(store):
    store.configure(7, True, 'gpt-6-sol', True, 1)
    finish_round(store)
    control = FakeControl(fail=True)
    store.apply_pending_pause(FakeAccounts(), control)
    state = store.public(7)
    assert state['pause_status'] == 'failed' and not state['paused_at'] and state['enabled']
    assert 'must-not-leak-secret' not in json.dumps(state)
    store.apply_pending_pause(FakeAccounts(), control)
    assert control.calls == [7]
    with store.db() as db:
        db.execute('UPDATE monitors SET pause_retry_at=0')
    control.fail = False
    s.Store(store.path).apply_pending_pause(FakeAccounts(), control)
    assert control.calls == [7, 7] and store.public(7)['pause_status'] == 'paused'


def test_native_success_before_process_crash_is_reconciled_without_second_write(store):
    store.configure(7, True, 'gpt-6-sol', True, 1)
    finish_round(store)
    accounts = FakeAccounts()
    accounts.account['schedulable'] = False
    control = FakeControl()
    s.Store(store.path).apply_pending_pause(accounts, control)
    assert control.calls == [] and store.public(7)['pause_status'] == 'paused'


def test_interrupted_probe_resets_streak_but_completed_results_survive(store):
    store.configure(7, True, 'gpt-6-sol', True, 3)
    finish_round(store)
    finish_round(store)
    claim(store)
    restarted = s.Store(store.path)
    restarted.recover()
    assert restarted.public(7)['mismatch_streak'] == 0
    assert not restarted.public(7)['running']
    assert len(restarted.public(7)['history']) == 2


def test_account_routing_changes_cancel_pending_action_and_streak(store):
    store.configure(7, True, 'gpt-6-sol', True, 2)
    finish_round(store)
    finish_round(store, context='new-route')
    assert store.public(7)['mismatch_streak'] == 1
    finish_round(store, context='new-route')
    control = FakeControl()
    store.apply_pending_pause(FakeAccounts(), control)
    assert control.calls == [] and store.public(7)['mismatch_streak'] == 0
    assert store.public(7)['pause_status'] == ''


def test_old_result_cannot_finish_later_claim(store):
    store.configure(7, True, 'gpt-6-sol', True, 3)
    old = finish_round(store)
    current = claim(store)
    store.finish(old, {'status': 'red', 'bank_applicability': 'validated', '_context': s.account_context(account()).hex()})
    assert store.public(7)['mismatch_streak'] == 1 and store.public(7)['running']
    store.finish(current, {'status': 'yellow'})
    assert store.public(7)['mismatch_streak'] == 0


@pytest.mark.parametrize('threshold', [0, 101, True, 2.5, '3', None])
def test_policy_api_rejects_invalid_threshold(store, threshold):
    client = s.create_app(store, FakeAccounts(), lambda h: True).test_client()
    assert client.put('/accounts/7', json=dict(enabled=True, model='gpt-6-sol',
        auto_pause_enabled=True, auto_pause_threshold=threshold)).status_code == 400
    assert store.get(7) is None


def test_policy_api_preserves_auto_settings_for_old_ui_and_allows_cancellation(store):
    accounts = FakeAccounts()
    client = s.create_app(store, accounts, lambda h: True).test_client()
    body = dict(enabled=True, model='gpt-6-sol', auto_pause_enabled=True, auto_pause_threshold=3)
    assert client.put('/accounts/7', json=body).status_code == 200
    first = store.get(7)
    assert client.put('/accounts/7', json=dict(enabled=True, model='gpt-6-sol')).status_code == 200
    assert store.get(7) == first
    accounts.metadata = lambda ids: {7: dict(id=7, platform='openai', type='oauth', status='active', schedulable=False, mapping={})}
    body['auto_pause_enabled'] = False
    assert client.put('/accounts/7', json=body).status_code == 200
    assert not store.public(7)['auto_pause_enabled']


def test_native_controller_only_posts_false_to_fixed_local_account_endpoint(tmp_path, monkeypatch):
    key = tmp_path / 'key'
    key.write_text('fixture-admin-key')
    calls = []
    class Session:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def post(self, url, **kwargs):
            assert self.trust_env is False
            calls.append((url, kwargs))
            return type('Response', (), dict(status_code=200, json=lambda _: {
                'code': 0, 'data': {'id': 7, 'schedulable': False}}))()
    monkeypatch.setattr(s.requests, 'Session', Session)
    s.SchedulingControl(key).pause(7)
    assert calls == [('http://127.0.0.1:8080/api/v1/admin/accounts/7/schedulable', {
        'headers': {'X-API-Key': 'fixture-admin-key'}, 'json': {'schedulable': False},
        'timeout': (1, 3), 'allow_redirects': False})]


@pytest.mark.parametrize('data', [{'id': 7, 'schedulable': True}, {'id': 8, 'schedulable': False}, {}])
def test_native_controller_requires_matching_pause_confirmation(tmp_path, monkeypatch, data):
    key = tmp_path / 'key'
    key.write_text('fixture-admin-key')
    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def post(self, *args, **kwargs):
            return type('Response', (), dict(status_code=200, json=lambda _: {'code': 0, 'data': data}))()
    monkeypatch.setattr(s.requests, 'Session', Session)
    with pytest.raises(s.ProbeError):
        s.SchedulingControl(key).pause(7)


def test_per_account_interval_controls_schedule_and_survives_restart(store, monkeypatch):
    monkeypatch.setattr(s.time, 'time', lambda: 10000)
    store.configure(7, True, 'gpt-6-sol', interval_minutes=2)
    store.configure(8, True, 'gpt-6-sol', interval_minutes=30)
    with store.db() as db:
        db.execute('UPDATE monitors SET next_run=0 WHERE account_id=7')
    job = store.claim()
    assert job['account_id'] == 7 and store.get(7)['next_run'] == 10120
    assert s.Store(store.path).public(7)['interval_minutes'] == 2
    assert s.Store(store.path).public(8)['interval'] == 1800
    store.finish(job, {'status': 'green'})
    with store.db() as db:
        db.execute('UPDATE monitors SET next_run=0 WHERE account_id=8')
    assert store.claim()['account_id'] == 8
    assert store.get(8)['next_run'] == 11800


def test_interval_only_change_preserves_inflight_round_and_red_streak(store, monkeypatch):
    store.configure(7, True, 'gpt-6-sol', True, 3)
    finish_round(store)
    job = claim(store)
    old = store.get(7)
    monkeypatch.setattr(s.time, 'time', lambda: 10000)
    store.configure(7, True, 'gpt-6-sol', True, 3, 30)
    new = store.get(7)
    assert new['generation'] == old['generation'] and new['running'] == 1
    assert new['mismatch_streak'] == 1 and new['next_run'] == 11800
    assert store.is_current(job)
    store.finish(job, {'status': 'red', 'bank_applicability': 'validated', '_context': s.account_context(account()).hex()})
    assert store.get(7)['mismatch_streak'] == 2 and store.get(7)['next_run'] == 11800
    store.configure(7, True, 'gpt-6-sol')  # Existing clients omit the new setting.
    assert store.get(7)['interval_minutes'] == 30 and store.get(7)['next_run'] == 11800


@pytest.mark.parametrize('minutes', [0, -1, 1441, True, 1.5, '10', None])
def test_interval_api_rejects_invalid_values(store, minutes):
    client = s.create_app(store, FakeAccounts(), lambda h: True).test_client()
    assert client.put('/accounts/7', json=dict(enabled=True, model='gpt-6-sol', interval_minutes=minutes)).status_code == 400
    assert store.get(7) is None


def test_interval_api_readback_and_legacy_clients(store):
    client = s.create_app(store, FakeAccounts(), lambda h: True).test_client()
    assert client.get('/accounts?ids=7').json['accounts']['7']['interval_minutes'] == 10
    for minutes in (1, 1440, 30):
        response = client.put('/accounts/7', json=dict(enabled=True, model='gpt-6-sol', interval_minutes=minutes))
        assert response.status_code == 200
        assert response.json['account']['interval_minutes'] == minutes
        assert response.json['account']['interval'] == minutes * 60
    before = store.get(7)
    client.put('/accounts/7', json=dict(enabled=True, model='gpt-6-sol'))
    assert store.get(7) == before


def prompt_claim(store, account_id=7):
    with store.db() as db:
        db.execute('UPDATE prompt_monitors SET next_run=0 WHERE account_id=?', (account_id,))
    return store.claim_prompt()


def test_prompt_test_defaults_persist_and_validate(store):
    client = s.create_app(store, FakeAccounts(), lambda h: True).test_client()
    initial = client.get('/accounts?ids=7').json['accounts']['7']['prompt_test']
    assert initial['enabled'] is False and initial['interval_minutes'] == 10
    assert initial['prompt'] == '{{challenge}}' and initial['challenge_count'] == 3
    response = client.put('/accounts/7/prompt-test', json={
        'enabled': True, 'model': 'gpt-6-sol', 'prompt': '请完成：\n{{challenge}}',
        'challenge_count': 2, 'interval_minutes': 15,
        'auto_pause_enabled': True, 'auto_pause_threshold': 2,
    })
    assert response.status_code == 200
    saved = response.json['account']['prompt_test']
    assert saved['enabled'] and saved['model'] == 'gpt-6-sol'
    assert saved['prompt'] == '请完成：\n{{challenge}}' and saved['challenge_count'] == 2
    assert saved['interval_minutes'] == 15 and saved['auto_pause_threshold'] == 2
    for body in (
        {'enabled': True, 'prompt': ''},
        {'enabled': True, 'prompt': 'hello'},
        {'enabled': True, 'prompt': '{{challenge}}{{challenge}}'},
        {'enabled': True, 'model': 'gpt-6.1-sol'},
        {'enabled': True, 'interval_minutes': 0},
        {'enabled': True, 'auto_pause_threshold': 101},
        {'enabled': True, 'challenge_count': True},
    ):
        assert client.put('/accounts/7/prompt-test', json=body).status_code == 400


def test_prompt_reds_trigger_only_prompt_auto_pause(store):
    store.configure_prompt(7, True, 'gpt-6-sol', '{{challenge}}', True, 2, 10)
    context = s.account_context(account()).hex()
    for _ in range(2):
        job = prompt_claim(store)
        store.finish_prompt(job, {'status': 'red', 'bank_applicability': 'validated', 'message': '复测仍不符', '_context': context})
    control = FakeControl()
    store.apply_pending_prompt_pause(FakeAccounts(), control)
    state = store.public(7)['prompt_test']
    assert control.calls == [7]
    assert state['pause_status'] == 'paused' and not state['enabled']
    assert state['failure_streak'] == 2 and state['history'][-1]['status'] == 'red'
    assert store.public(7)['enabled'] is False


@pytest.mark.parametrize('status', ['green', 'yellow'])
def test_prompt_non_red_breaks_streak(store, status):
    store.configure_prompt(7, True, 'gpt-6-sol', '{{challenge}}', True, 2)
    for color in ['red', status, 'red']:
        store.finish_prompt(prompt_claim(store), {'status': color, 'bank_applicability': 'validated', '_context': s.account_context(account()).hex()})
    control = FakeControl()
    store.apply_pending_prompt_pause(FakeAccounts(), control)
    assert control.calls == [] and store.public(7)['prompt_test']['failure_streak'] == 1


def test_prompt_save_idempotent_interval_only_and_cancel(store):
    store.configure_prompt(7, True, 'gpt-6-sol', '{{challenge}}', True, 3)
    job = prompt_claim(store)
    before = store.get_prompt(7)
    store.configure_prompt(7, True, 'gpt-6-sol', '{{challenge}}')
    assert store.get_prompt(7) == before
    store.configure_prompt(7, True, 'gpt-6-sol', '{{challenge}}', interval_minutes=20)
    assert store.is_prompt_current(job)
    store.configure_prompt(7, True, 'gpt-6-sol', '新模板 {{challenge}}')
    assert not store.is_prompt_current(job)
    store.finish_prompt(job, {'status': 'red'})
    assert store.public(7)['prompt_test']['history'] == []


class PromptControl(FakeControl):
    def __init__(self, text=None, error=None):
        super().__init__()
        self.text = text if text is not None else ','.join(['137'] * 333)
        self.error = error
        self.prompts = []
        self.support_checks = 0

    def check_prompt_support(self, reasoning_effort='xhigh'):
        self.support_checks += 1

    def prompt(self, account_id, model, prompt, cancelled, deadline, *, reasoning_effort='xhigh'):
        self.prompts.append((account_id, model, prompt))
        if self.error:
            raise self.error
        return self.text


@pytest.mark.parametrize('predictions,status,requests', [
    (['gpt-6-sol'], 'green', 2),
    (['gpt-5.4', 'gpt-5.4'], 'red', 2),
    (['gpt-5.4', 'gpt-6-sol'], 'red', 2),
])
def test_prompt_uses_one_attribution_batch_without_retest(store, monkeypatch, predictions, status, requests):
    store.configure_prompt(7, True, 'gpt-6-sol', '测试前言\n{{challenge}}', challenge_count=2)
    answers = iter(predictions)
    def summarize(outputs, model):
        assert len(outputs) == 2
        prediction = next(answers)
        return dict(status='green' if prediction == model else 'yellow', prediction=prediction,
                    probability=.95, confident=True)
    monkeypatch.setattr(s, 'summarize', summarize)
    control = PromptControl()
    result = s.run_prompt_round(prompt_claim(store), store, FakeAccounts(), control)
    assert result['status'] == status and result['requests'] == requests
    assert len(control.prompts) == requests and control.support_checks == 1
    assert all(p[0] == 7 and p[2].startswith('测试前言\n') and '{{challenge}}' not in p[2] for p in control.prompts)
    assert len(set(p[2] for p in control.prompts)) == requests


@pytest.mark.parametrize('text,error,count', [('hello', None, 3), (None, s.ProbeError('等待上游响应超时'), 1)])
def test_prompt_insufficient_and_timeout_stay_yellow(store, text, error, count):
    store.configure_prompt(7, True, 'gpt-6-sol', '{{challenge}}', True, 1)
    control = PromptControl(text, error)
    job = prompt_claim(store)
    result = s.run_prompt_round(job, store, FakeAccounts(), control)
    store.finish_prompt(job, result)
    store.apply_pending_prompt_pause(FakeAccounts(), control)
    assert result['status'] == 'yellow' and len(control.prompts) == count
    assert control.calls == [] and store.public(7)['prompt_test']['failure_streak'] == 0


def test_prompt_disable_with_invalid_form_and_unavailable_account(store):
    store.configure_prompt(7, True, 'gpt-6-sol', '{{challenge}}')
    accounts = FakeAccounts()
    accounts.metadata = lambda ids: {7: dict(id=7, platform='openai', type='oauth', status='error', schedulable=False)}
    client = s.create_app(store, accounts, lambda h: True).test_client()
    assert client.put('/accounts/7/prompt-test', json={'enabled': False, 'prompt': ''}).status_code == 200
    assert not store.get_prompt(7)['enabled']


def test_prompt_full_length_chinese_payload_fits_limit(store):
    client = s.create_app(store, FakeAccounts(), lambda h: True).test_client()
    body = dict(enabled=True, model='gpt-6-sol', prompt='中' * (4000 - len('{{challenge}}')) + '{{challenge}}')
    assert client.put('/accounts/7/prompt-test', data=json.dumps(body, ensure_ascii=False).encode(), content_type='application/json').status_code == 200


@pytest.mark.parametrize('effort', ['low', 'medium', 'high', 'xhigh'])
def test_prompt_api_contract_and_stream_completion(tmp_path, monkeypatch, effort):
    profile = s.comparison_profile(effort)
    key = tmp_path / 'key'; key.write_text('fixture-admin-key')
    calls = []
    class Stream(Response):
        status_code = 200
        def __enter__(self): return self
        def __exit__(self, *args): pass
    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def post(self, url, **kwargs):
            calls.append((url, kwargs))
            identity = kwargs['headers']['X-Client-Request-Id']
            assert kwargs['headers']['X-Sub2API-Comparison-Profile'] == profile
            return Stream([
                {'type': 'test_request', 'data': {'client_request_id': identity, 'upstream_attempt_id': identity, 'profile': profile,
                 'details': {'account_id': 7, 'transport': 'codex-http1-openssl36', 'body': {
                     'model': 'gpt-6-sol', 'reasoning': {'effort': effort}, 'instructions': s.COMPARISON_INSTRUCTIONS,
                     'tools': [], 'tool_choice': 'none', 'parallel_tool_calls': False, 'text': {'verbosity': 'medium'},
                     'input': [{'role': 'user', 'content': [{'type': 'input_text', 'text': '自定义测试'}]}]}}}},
                {'type': 'test_response', 'data': {'client_request_id': identity, 'upstream_attempt_id': identity,
                                                'profile': profile, 'details': {'http_status': 200}}},
                {'type': 'content', 'text': '123'}, {'type': 'test_complete', 'success': True}])
    monkeypatch.setattr(s.requests, 'Session', Session)
    control = s.SchedulingControl(key)
    assert control.prompt(7, 'gpt-6-sol', '自定义测试', reasoning_effort=effort) == '123'
    assert calls[0][0].endswith('/accounts/7/test')
    assert calls[0][1]['json'] == dict(model_id='gpt-6-sol', prompt='自定义测试', mode='attribution')
    assert calls[0][1]['allow_redirects'] is False


def test_prompt_missing_capability_does_not_post(tmp_path, monkeypatch):
    key = tmp_path / 'key'; key.write_text('fixture-admin-key')
    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def get(self, *args, **kwargs):
            return type('OldGateway', (), {'status_code': 404})()
        def post(self, *args, **kwargs):
            pytest.fail('must not send a model request')
    monkeypatch.setattr(s.requests, 'Session', Session)
    with pytest.raises(s.ProbeError, match='未发送模型请求'):
        s.SchedulingControl(key).check_prompt_support()


def test_prompt_incompatible_gateway_records_zero_requests(store):
    store.configure_prompt(7, True, 'gpt-6-sol', '{{challenge}}')
    control = PromptControl()
    def unavailable(reasoning_effort='xhigh'):
        raise s.ProbeError('接口未支持')
    control.check_prompt_support = unavailable
    result = s.run_prompt_round(prompt_claim(store), store, FakeAccounts(), control)
    assert result['status'] == 'yellow' and result['requests'] == 0
    assert control.prompts == []


@pytest.mark.parametrize('count', [0, 4, -1, True, 1.5, '2', None])
def test_challenge_count_api_rejects_invalid_values(store, count):
    client = s.create_app(store, FakeAccounts(), lambda h: True).test_client()
    response = client.put('/accounts/7', json=dict(enabled=True, model='gpt-6-sol', challenge_count=count))
    assert response.status_code == 400
    assert store.get(7) is None


def test_challenge_count_is_per_account_persisted_and_preserved_by_old_clients(store):
    client = s.create_app(store, FakeAccounts(), lambda h: True).test_client()
    assert client.get('/accounts?ids=7').json['accounts']['7']['challenge_count'] == 3
    for count in (1, 2, 3):
        response = client.put('/accounts/7', json=dict(enabled=True, model='gpt-6-sol', challenge_count=count))
        assert response.status_code == 200
        assert response.json['account']['challenge_count'] == count
    store.configure(7, True, 'gpt-6-sol', challenge_count=2)
    store.configure(8, True, 'gpt-6-sol')
    before = store.get(7)
    client.put('/accounts/7', json=dict(enabled=True, model='gpt-6-sol'))
    assert store.get(7) == before
    assert s.Store(store.path).public(7)['challenge_count'] == 2
    assert s.Store(store.path).public(8)['challenge_count'] == 3


def test_challenge_count_change_cancels_old_result_without_earlier_paid_round(store):
    store.configure(7, True, 'gpt-6-sol', True, 3)
    finish_round(store)
    job = claim(store)
    before = store.get(7)
    store.configure(7, True, 'gpt-6-sol', challenge_count=1)
    after = store.get(7)
    assert after['next_run'] == before['next_run']
    assert after['mismatch_streak'] == 0 and after['pause_status'] == ''
    assert after['auto_pause_enabled'] == 1 and after['auto_pause_threshold'] == 3
    assert not store.is_current(job)
    store.finish(job, dict(status='red', _context=s.account_context(account()).hex()))
    assert len(store.public(7)['history']) == 1
    assert store.claim() is None


@pytest.mark.parametrize('count', [1, 2, 3])
@pytest.mark.parametrize('outcome', ['match', 'mismatch', 'insufficient', 'replaced_mismatch'])
def test_selected_challenge_count_is_the_strict_request_limit(store, monkeypatch, count, outcome):
    store.configure(7, True, 'gpt-6-sol', True, 1, challenge_count=count)
    calls, scored = [], []
    def probe(a, model, challenge, *args):
        calls.append(challenge)
        if outcome == 'insufficient' or (outcome == 'replaced_mismatch' and len(calls) == 1):
            raise s.InsufficientSample('有效数字样本不足')
        return {'text': str(len(calls)), 'expected_count': 300}, model
    def summarize(outputs, model):
        scored.append(outputs)
        return dict(status='green' if outcome == 'match' else 'yellow', confident=True,
                    bank_applicability='validated',
                    prediction=model if outcome == 'match' else 'gpt-5.4', probability=.99)
    monkeypatch.setattr(s, 'summarize', summarize)
    job = claim(store)
    result = s.run_round(job, store, FakeAccounts(), '0.163.0', probe)
    expected_requests = count
    assert result['requests'] == len(calls) == expected_requests
    assert result['challenge_count'] == count and result['request_limit'] == count
    assert all(len(outputs) == count for outputs in scored)
    assert len({id(c) for c in calls}) == len(calls)
    if outcome == 'mismatch':
        assert result['status'] == 'red' and len(scored) == 1
    elif outcome == 'match':
        assert result['status'] == 'green' and len(scored) == 1
    else:
        assert result['status'] == 'yellow' and not scored
    assert 'confirmation' not in result
    store.finish(job, result)
    control = FakeControl()
    store.apply_pending_pause(FakeAccounts(), control)
    assert control.calls == ([7] if outcome == 'mismatch' else [])


@pytest.mark.parametrize('count', [1, 2, 3])
@pytest.mark.parametrize('probability,second,used_delta', [(.95,.02,0), (.79,.1,0), (.8,.6,0), (.95,.02,-1)])
def test_selected_count_retains_score_and_margin_thresholds(monkeypatch, count, probability, second, used_delta):
    monkeypatch.setattr(s, 'analyze_global_outputs', lambda *args: dict(
        used_outputs=count+used_delta, probability=probability, prediction='gpt-6-sol',
        results=[dict(model='gpt-6-sol', probability=probability), dict(model='gpt-5.4', probability=second)]))
    result = s.summarize([{}] * count, 'gpt-6-sol')
    assert result['candidate_separated'] == (probability >= .8 and probability - second >= .25 and used_delta == 0)
    assert result['status'] == ('green' if result['candidate_separated'] else 'yellow')
    assert not result['confident'] and result['bank_applicability'] == 'unverified'
