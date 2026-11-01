"""Selective evidence persistence; no live models or production writes."""
import json

import pytest

import attempts
import evidence
import service as s
from test_service import FakeAccounts, claim, prompt_claim, store
from test_qa import Answers, configure, question


class WireAnswers(Answers):
    def prompt(self, account_id, model, prompt, cancelled, deadline, **kwargs):
        attempts.emit('gateway_request', {'profile': s.comparison_profile(kwargs['reasoning_effort']),
            'client_request_id': attempts.ACTIVE.get()['id'], 'upstream_attempt_id': 'linked-upstream',
            'details': {'account_id': account_id, 'proxy_id': 11, 'transport': 'codex-http1-openssl36',
                'url': 'https://user:password@chatgpt.com/backend-api/codex/responses?token=hidden',
                'headers': {'Authorization': 'Bearer admin-secret', 'Cookie': 'cookie-secret',
                            'Version': '0.166.0', 'X-Request-Id': 'upstream-id'},
                'body': {'model': model, 'input': prompt, 'instructions': 'fixed instructions',
                         'reasoning': {'effort': kwargs['reasoning_effort']}}}})
        attempts.stream_line(b'data: NEVER-RETAIN-STREAM')
        return super().prompt(account_id, model, prompt, cancelled, deadline, **kwargs)


def qa(store, *answers, accounts=None):
    job = prompt_claim(store)
    result = s.run_prompt_round(job, store, accounts or FakeAccounts(), WireAnswers(*answers))
    store.finish_prompt(job, result)
    return job, result


def diagnostics(store):
    with store.db() as db:
        return [r[0] for r in db.execute("SELECT payload FROM probe_attempt_events WHERE kind='diagnostic' ORDER BY sequence")]


def test_only_first_normal_and_anomaly_survive_restarts_and_proxy_changes(store):
    configure(store)
    _, normal = qa(store, '21')
    baseline = diagnostics(store)
    assert len(baseline) == 1
    assert evidence.listing(store, 7)[0]['slot'] == 'normal'
    encoded = baseline[0]
    assert 'fixed instructions' in encoded and 'linked-upstream' in encoded
    assert all(secret not in encoded for secret in ('admin-secret', 'cookie-secret', 'user:password', 'token=hidden', 'NEVER-RETAIN-STREAM'))
    fresh = s.Store(store.path)
    accounts = FakeAccounts()
    accounts.account.update(proxy_id=99, extra={'codex_timezone': 'America/Chicago'})
    for _ in range(3):
        qa(fresh, '21', accounts=accounts)
    _, anomaly = qa(fresh, '29', '29', accounts=accounts)
    kept = diagnostics(fresh)
    assert len(kept) == 2 and kept[0] == baseline[0]
    for _ in range(3):
        qa(fresh, '29', '29', accounts=accounts)
    assert diagnostics(fresh) == kept
    slots = evidence.listing(fresh, 7)
    assert {r['slot'] for r in slots} == {'normal', 'anomaly'} and len({r['condition_sha256'] for r in slots}) == 1
    assert slots[0]['run_id'] == anomaly['run_id'] and slots[1]['run_id'] == normal['run_id']


def test_repeated_long_answers_only_keep_preview_and_first_full_evidence(store):
    configure(store)
    long = 'wrong answer ' * 40000
    qa(store, long, long)
    saved = diagnostics(store)
    _, repeated = qa(store, long, long)
    assert diagnostics(store) == saved
    history = store.public(7)['prompt_test']['history']
    assert len(json.dumps(history)) < 6000
    first_answer = history[0]['cases'][0]['attempts'][0]
    repeat_answer = history[1]['cases'][0]['attempts'][0]
    assert first_answer['evidence_saved'] and not repeat_answer['evidence_saved']
    assert evidence.answer(store, 7, first_answer['attempt_id'])['output'] == long
    assert evidence.answer(store, 7, repeat_answer['attempt_id']) == {'output': long[:160], 'output_truncated': True}
    with store.db() as db:
        size = db.execute("SELECT sum(length(payload)) FROM probe_attempt_events WHERE attempt_id IN (SELECT attempt_id FROM probe_attempts WHERE run_id=?)", (repeated['run_id'],)).fetchone()[0]
    assert size < 5000


def test_each_question_retains_anomaly_even_when_round_is_yellow(store):
    configure(store, [question(), question('tibo', expected='True'), question('later')])
    _, result = qa(store, '29', 'False', s.RoundTimeout())
    assert result['status'] == 'yellow'
    records = evidence.listing(store, 7)
    assert {(r['summary']['case_id'], r['slot']) for r in records} == {('candy', 'anomaly'), ('tibo', 'anomaly')}
    assert len(diagnostics(store)) == 2


def test_finished_case_is_preserved_when_later_question_times_out(store):
    configure(store, [question(), question('tibo', expected='True')])
    qa(store, '29', s.RoundTimeout())
    records = evidence.listing(store, 7)
    assert len(records) == 1 and records[0]['summary']['case_id'] == 'candy'


def test_first_wrong_answer_is_kept_without_attempting_confirmation(store):
    configure(store)
    qa(store, '29', s.RoundTimeout())
    records = evidence.listing(store, 7)
    assert len(records) == 1 and records[0]['slot'] == 'anomaly'
    assert records[0]['summary']['status'] == 'red'
    assert evidence.answer(store, 7, records[0]['summary']['attempt_ids'][0])['output'] == '29'


def test_timeout_without_any_judgeable_answer_does_not_create_evidence(store):
    configure(store)
    qa(store, s.RoundTimeout())
    assert not evidence.listing(store, 7) and not diagnostics(store)


def test_stale_or_changed_comparison_cannot_consume_a_slot(store):
    configure(store)
    job = prompt_claim(store)
    result = s.run_prompt_round(job, store, FakeAccounts(), WireAnswers('21'))
    store.configure_prompt(7, False, job['model'], s.DEFAULT_PROMPT)
    store.finish_prompt(job, result)
    assert not diagnostics(store) and not evidence.listing(store, 7)
    configure(store)
    job = prompt_claim(store)
    result = s.run_prompt_round(job, store, FakeAccounts(), WireAnswers('21'))
    result['outcome'] = 'comparison_changed'
    store.finish_prompt(job, result)
    assert not diagnostics(store) and not evidence.listing(store, 7)


@pytest.mark.parametrize('change', ['effort', 'question', 'model', 'expected'])
def test_different_comparison_conditions_have_separate_slots(store, change):
    configure(store)
    qa(store, '21')
    case = question()
    if change == 'question': case['prompt'] += ' now'
    if change == 'expected': case['expected_answer'] = '22'
    store.configure_prompt(7, True, 'gpt-6-astra' if change == 'model' else 'gpt-6.1-sol', s.DEFAULT_PROMPT,
                           test_mode='qa', cases=[case], reasoning_effort='low' if change == 'effort' else 'xhigh')
    qa(store, case['expected_answer'])
    assert len(evidence.listing(store, 7)) == 2


def test_attribution_match_and_first_separated_mismatch_capture_without_certifying_identity(store, monkeypatch):
    store.configure(7, True, 'gpt-6-astra', challenge_count=1)
    prediction = 'gpt-6-astra'
    monkeypatch.setattr(s, 'summarize', lambda *_: dict(status='yellow', prediction=prediction, probability=.99,
        confident=False, candidate_separated=True, bank_applicability='unverified', bank_sha256='bank', analysis={}))
    for model in ['gpt-6-astra', 'gpt-6-astra', 'gpt-5.6-luna', 'gpt-5.6-luna']:
        prediction = model
        job = claim(store)
        result = s.run_round(job, store, FakeAccounts(), '0.166.0', lambda *_: ({'text':'full numbers'}, job['model']))
        store.finish(job, result)
    records = evidence.listing(store, 7)
    assert len(records) == 2 and len(diagnostics(store)) == 2
    assert all(r['summary']['bank_applicability'] == 'unverified' for r in records)
    assert records[0]['summary']['prediction'] == 'gpt-5.6-luna'


def test_answer_and_archive_api_are_authenticated_and_account_scoped(store):
    configure(store)
    qa(store, '21')
    identity = evidence.listing(store, 7)[0]['summary']['attempt_ids'][0]
    client = s.create_app(store, FakeAccounts(), lambda h: h.get('Authorization') == 'ok').test_client()
    path = f'/accounts/7/attempts/{identity}/answer'
    assert client.get(path).status_code == 401
    assert client.get(path, headers={'Authorization': 'ok'}).json['output'] == '21'
    assert client.get(path.replace('/7/', '/8/'), headers={'Authorization': 'ok'}).status_code == 404
    listing = client.get('/accounts/7/attempts', headers={'Authorization': 'ok'}).json
    assert listing['saved_evidence'][0]['slot'] == 'normal'
    assert listing['attempts'][0]['metadata']['capture_slot'] == 'normal'
    assert not evidence.listing(store, 8)
