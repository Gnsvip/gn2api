"""Offline regressions for the observed slow xhigh rounds and selected effort."""
import pytest

import attempts
import service as s
from test_service import FakeAccounts, PromptControl, claim, prompt_claim, store


def test_legacy_sqlite_migration_preserves_history_and_settings(store):
    from upgrade_prompt_test import existing_state, state_preserved
    store.configure(7, True, 'gpt-6-astra', interval_minutes=5)
    store.configure_prompt(7, False, 'gpt-6-astra', '{{challenge}}')
    store.finish(claim(store), {'status': 'yellow', 'message': 'original history'})
    with store.db() as db:
        for table in ('monitors', 'prompt_monitors'):
            db.execute(f'ALTER TABLE {table} DROP COLUMN reasoning_effort')
        before = existing_state(db)
    migrated = s.Store(store.path)
    with migrated.db() as db:
        assert state_preserved(db, before)
    assert migrated.get(7)['reasoning_effort'] == 'xhigh'
    assert migrated.get_prompt(7)['reasoning_effort'] == 'xhigh'


def test_three_slow_valid_samples_finish_without_lowering_effort(store, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(s.time, 'monotonic', lambda: clock[0])
    store.configure(7, True, 'gpt-6-astra')
    # This case covers the initial budget; a mismatch correctly starts a second stage.
    monkeypatch.setattr(s, 'analyze_global_outputs', lambda outputs, bank: {
        'results': [{'model':'gpt-6-astra','probability':.99}, {'model':'other','probability':.01}],
        'prediction':'gpt-6-astra','probability':.99,'used_outputs':len(outputs)})
    deadlines = []

    def slow_probe(account, model, challenge, cancelled, deadline, version):
        deadlines.append(deadline)
        clock[0] += 173
        return {'text': ','.join(['137'] * 333), 'expected_count': 333}, model

    result = s.run_round(claim(store), store, FakeAccounts(), '', slow_probe)
    assert result['valid_samples'] == result['requests'] == 3
    assert result['elapsed_seconds'] == 519
    assert deadlines == [1720] * 3
    assert result['budget_seconds'] == 720 and result['reasoning_effort'] == 'xhigh'
    assert result['outcome'] == 'bank_unverified' and not result['confident']
    assert len(attempts.listing(store, 7)['attempts']) == 3


@pytest.mark.parametrize('duration,expected_requests,budget', [(721, 1, 720), (300, 4, 1200)])
def test_expired_round_cannot_be_revived_by_replacement(store, monkeypatch, duration, expected_requests, budget):
    clock = [1000.0]
    monkeypatch.setattr(s.time, 'monotonic', lambda: clock[0])
    store.configure(7, True, 'gpt-6-astra')
    deadlines = []

    def insufficient(*args):
        deadlines.append(args[-2])
        clock[0] += duration
        raise s.InsufficientSample('有效数字样本不足')

    result = s.run_round(claim(store), store, FakeAccounts(), '', insufficient)
    assert result['outcome'] == 'round_timeout'
    assert result['requests'] == expected_requests and result['budget_seconds'] == budget
    assert deadlines[0] == 1720 and all(d == 2200 for d in deadlines[1:])


def test_retest_uses_one_twenty_minute_cap_from_round_start(store, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(s.time, 'monotonic', lambda: clock[0])
    store.configure(7, True, 'gpt-6-astra')
    deadlines = []

    def slow_probe(account, model, challenge, cancelled, deadline, version):
        deadlines.append(deadline)
        clock[0] += 173
        return {'text': 'fixture', 'expected_count': 300}, model

    # Exercise the confirmation branch without pretending the installed bank is calibrated.
    monkeypatch.setattr(s, 'summarize', lambda *args: dict(status='yellow', prediction='fixture-other', confident=True, probability=.99))
    result = s.run_round(claim(store), store, FakeAccounts(), '', slow_probe)
    assert result['requests'] == 6 and result['elapsed_seconds'] == 1038
    assert deadlines == [1720] * 3 + [2200] * 3
    assert result['budget_seconds'] == 1200


@pytest.mark.parametrize('prompt', [False, True])
def test_effort_saved_per_monitor_cancels_old_round_and_survives_legacy_save(store, prompt):
    client = s.create_app(store, FakeAccounts(), lambda _: True).test_client()
    url = '/accounts/7' + ('/prompt-test' if prompt else '')
    body = dict(enabled=True, model='gpt-6-astra')
    if prompt:
        body.update(test_mode='attribution', prompt='{{challenge}}')
    assert client.put(url, json=body).status_code == 200
    job = prompt_claim(store) if prompt else claim(store)
    getter = store.get_prompt if prompt else store.get
    scheduled_at = getter(7)['next_run']
    assert job['reasoning_effort'] == 'xhigh'
    response = client.put(url, json={**body, 'reasoning_effort': 'medium'})
    assert response.status_code == 200
    assert not (store.is_prompt_current(job) if prompt else store.is_current(job))
    assert client.put(url, json=body).status_code == 200  # Old clients must not reset medium.
    state = s.Store(store.path).public(7)
    selected = state['prompt_test'] if prompt else state
    assert selected['reasoning_effort'] == 'medium'
    assert (state['reasoning_effort'] if prompt else state['prompt_test']['reasoning_effort']) == 'xhigh'
    assert getter(7)['next_run'] == scheduled_at


@pytest.mark.parametrize('prompt', [False, True])
@pytest.mark.parametrize('invalid', ['', 'default', 'ultra', None, True, [], {}])
def test_api_rejects_unsupported_effort_without_saving(store, prompt, invalid):
    client = s.create_app(store, FakeAccounts(), lambda _: True).test_client()
    url = '/accounts/7' + ('/prompt-test' if prompt else '')
    response = client.put(url, json=dict(enabled=False, model='gpt-6-astra', reasoning_effort=invalid))
    assert response.status_code == 400
    assert (store.get_prompt(7) if prompt else store.get(7)) is None


@pytest.mark.parametrize('mode', ['attribution', 'qa'])
def test_selected_effort_reaches_controller_and_append_only_evidence(store, mode):
    store.configure_prompt(7, True, 'gpt-6-astra', '{{challenge}}', reasoning_effort='high', test_mode=mode,
                           cases=[dict(id='number', name='number', enabled=True, prompt='Only answer 21', expected_answer='21')])

    class Capture(PromptControl):
        def check_prompt_support(self, reasoning_effort='xhigh'):
            assert reasoning_effort == 'high'

        def prompt(self, account_id, model, prompt, cancelled, deadline, *, reasoning_effort='xhigh'):
            assert reasoning_effort == 'high'
            return '21' if mode == 'qa' else ','.join(['137'] * 333)

    job = prompt_claim(store)
    result = s.run_prompt_round(job, store, FakeAccounts(), Capture())
    assert result['requests'] > 0 and result['reasoning_effort'] == 'high'
    entries = attempts.listing(store, 7)['attempts']
    assert all(entry['metadata']['reasoning_effort'] == 'high' for entry in entries)
    store.finish_prompt(job, result)
    assert store.public(7)['prompt_test']['history'][-1]['reasoning_effort'] == 'high'


@pytest.mark.parametrize('prompt', [False, True])
def test_slow_round_waits_an_interval_instead_of_immediate_catchup(store, monkeypatch, prompt):
    now = [10000.0]
    monkeypatch.setattr(s.time, 'time', lambda: now[0])
    if prompt:
        store.configure_prompt(7, True, 'gpt-6-astra', '{{challenge}}', interval_minutes=5)
        job = prompt_claim(store)
    else:
        store.configure(7, True, 'gpt-6-astra', interval_minutes=5)
        job = claim(store)
    now[0] += 519
    (store.finish_prompt if prompt else store.finish)(job, {'status': 'yellow'})
    assert (store.get_prompt if prompt else store.get)(7)['next_run'] == now[0] + 300
    assert (store.claim_prompt if prompt else store.claim)() is None


def test_old_gateway_cannot_silently_run_selected_low_as_xhigh(tmp_path, monkeypatch):
    key = tmp_path / 'key'
    key.write_text('fixture')

    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def get(self, *args, **kwargs):
            return type('Response', (), {'status_code': 200, 'json': lambda _: {
                'code': 0, 'data': {'comparison_audit_v1': True, 'comparison_profiles': [s.COMPARISON_PROFILE]}}})()
        def post(self, *args, **kwargs):
            pytest.fail('no paid request allowed for unsupported effort')

    monkeypatch.setattr(s.requests, 'Session', Session)
    control = s.SchedulingControl(key)
    control.check_prompt_support('xhigh')
    with pytest.raises(s.ProbeError) as error:
        control.check_prompt_support('low')
    assert error.value.code == 'capability_unavailable'


def test_round_timeout_keeps_readable_attempt_evidence(store):
    job = dict(account_id=7, model='gpt-6-astra', generation=1)
    def timeout():
        raise s.RoundTimeout()
    with pytest.raises(s.RoundTimeout):
        attempts.invoke(store, job, 'timeout', FakeAccounts().get(7), {}, timeout)
    finished = attempts.listing(store, 7)['attempts'][0]['finished']
    assert finished['error_code'] == 'round_timeout' and '预算' in finished['error']
