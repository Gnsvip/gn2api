"""Single-red policy, current-turn human reviews, and non-destructive preset upgrade."""
import copy
import json

import pytest
import service as s
from test_qa import Answers, configure, question
from test_service import FakeAccounts, account, prompt_claim, store

DOC = '<svg xmlns="http://www.w3.org/2000/svg"><circle r="10"/></svg>'


def manual_round(store, *, enabled=True, extra=None):
    cases = [{**question('drawing', expected=''), 'kind': 'manual'}]
    if extra:
        cases.append(extra)
    configure(store, cases, auto_pause_enabled=enabled)
    control = Answers(DOC, DOC)
    job = prompt_claim(store)
    store.finish_prompt(job, s.run_prompt_round(job, store, FakeAccounts(), control))
    return store.public(7)['prompt_test']['history'][-1]['result_id'], control


def test_first_automatic_red_stops_once_even_when_old_client_submits_three(store):
    configure(store, auto_pause_enabled=True, auto_pause_threshold=3)
    control = Answers('29')
    job = prompt_claim(store)
    store.finish_prompt(job, s.run_prompt_round(job, store, FakeAccounts(), control))
    store.apply_pending_prompt_pause(FakeAccounts(), control)
    store.apply_pending_prompt_pause(FakeAccounts(), control)
    assert control.calls == [7] and len(control.prompts) == 1
    assert store.get_prompt(7)['auto_pause_threshold'] == 1
    assert store.get_prompt(7)['pause_status'] == 'paused'


def test_current_manual_red_stops_immediately_via_review_api_and_never_resumes(store):
    result_id, control = manual_round(store, extra={**question('second', expected=''), 'kind': 'manual'})
    client = s.create_app(store, FakeAccounts(), lambda _: True, control).test_client()
    url = f'/accounts/7/prompt-test/results/{result_id}/reviews/drawing'
    response = client.put(url, json={'status': 'red'})
    assert response.status_code == 200
    state = response.json['account']['prompt_test']
    assert control.calls == [7] and not state['enabled'] and state['pause_status'] == 'paused'
    assert state['history'][-1]['status'] == 'red'
    assert '_pause_guard' not in state['history'][-1]
    client.put(url, json={'status': 'red'})
    client.put(url, json={'status': 'green'})
    assert control.calls == [7] and store.get_prompt(7)['pause_status'] == 'paused'


@pytest.mark.parametrize('change', ['disabled', 'old_result', 'new_running', 'settings', 'legacy', 'route'])
def test_stale_or_disarmed_manual_reviews_do_not_stop_account(store, change):
    result_id, control = manual_round(store, enabled=change != 'disabled')
    accounts = FakeAccounts()
    if change == 'old_result':
        job = prompt_claim(store)
        store.finish_prompt(job, s.run_prompt_round(job, store, accounts, Answers(DOC)))
    elif change == 'new_running':
        prompt_claim(store)
    elif change == 'settings':
        old = store.get_prompt(7)
        store.configure_prompt(7, True, old['model'], old['prompt'], reasoning_effort='high')
    elif change == 'legacy':
        with store.db() as db:
            result = json.loads(db.execute('SELECT result FROM prompt_results WHERE id=?', (result_id,)).fetchone()[0])
            result.pop('_pause_guard')
            db.execute('UPDATE prompt_results SET result=? WHERE id=?', (json.dumps(result), result_id))
    elif change == 'route':
        accounts.get = lambda _: {**account(), 'proxy_id': 987}
    client = s.create_app(store, accounts, lambda _: True, control).test_client()
    assert client.put(f'/accounts/7/prompt-test/results/{result_id}/reviews/drawing', json={'status': 'red'}).status_code == 200
    store.apply_pending_prompt_pause(accounts, control)
    assert not control.calls


def test_review_correction_cancels_queued_pause_before_it_is_applied(store):
    result_id, control = manual_round(store)
    store.review_artifact(7, result_id, 'drawing', 'red')
    assert store.get_prompt(7)['pause_status'] == 'pending'
    store.review_artifact(7, result_id, 'drawing', 'yellow')
    store.apply_pending_prompt_pause(FakeAccounts(), control)
    assert not control.calls and store.get_prompt(7)['failure_streak'] == 0


def test_incomplete_round_manual_red_is_saved_without_stopping(store):
    configure(store, [{**question('drawing', expected=''), 'kind': 'manual'}, question('failed')], auto_pause_enabled=True)
    control = Answers(DOC, s.ProbeError('upstream interrupted', 'stream_interrupted'))
    job = prompt_claim(store)
    store.finish_prompt(job, s.run_prompt_round(job, store, FakeAccounts(), control))
    result_id = store.public(7)['prompt_test']['history'][-1]['result_id']
    store.review_artifact(7, result_id, 'drawing', 'red')
    store.apply_pending_prompt_pause(FakeAccounts(), control)
    assert store.public(7)['prompt_test']['history'][-1]['status'] == 'yellow'
    assert not control.calls


def test_automatic_red_with_pending_artwork_counts_without_waiting_for_review(store):
    configure(store, [question(), {**question('drawing', expected=''), 'kind': 'manual'}], auto_pause_enabled=True)
    control = Answers('29', DOC)
    job = prompt_claim(store)
    store.finish_prompt(job, s.run_prompt_round(job, store, FakeAccounts(), control))
    store.apply_pending_prompt_pause(FakeAccounts(), control)
    assert control.calls == [7]


def test_candy_preset_contains_original_question_without_reinterpreting_conditions(store):
    prompt = store.public(7)['prompt_test']['qa_presets'][0]['prompt']
    assert prompt.startswith('在一个黑色的袋子里放有三种口味的糖果，每种糖果有两种不同的形状（圆形和五角星形，不同的形状靠手感可以分辨）。')
    assert '参赛者需要在活动前决定摸出的糖果数目' in prompt
    assert '（同时手中有圆形苹果味匹配五角星桃子味糖果，或者有圆形桃子味匹配五角星苹果味糖果都满足要求）' in prompt
    assert '苹果味 桃子味 西瓜味\n\n圆形 7 9 8\n\n五角星形 7 6 4' in prompt
    assert '选择形状' not in prompt and '不能靠手感辨别口味' not in prompt
    assert prompt.endswith('只回答最少颗数的阿拉伯数字，不要解释。')


def test_upgrade_preserves_history_custom_edits_and_stopped_accounts(store):
    from upgrade_prompt_test import existing_state, state_preserved
    original = {**copy.deepcopy(s.DEFAULT_QA_CASES[0]), 'prompt': s.LEGACY_CANDY_PROMPT, 'enabled': True}
    configure(store, [original], auto_pause_enabled=True)
    job = prompt_claim(store)
    store.finish_prompt(job, s.run_prompt_round(job, store, FakeAccounts(), Answers('29')))
    history = store.public(7)['prompt_test']['history']
    with store.db() as db:
        db.execute('UPDATE prompt_monitors SET auto_pause_threshold=3 WHERE account_id=7')
    for identity, custom in [(8, {**original, 'prompt': '自定义糖果题'}), (9, {**original, 'expected_answer': '29'})]:
        store.configure_prompt(identity, False, 'gpt-6-sol', s.DEFAULT_PROMPT, test_mode='qa', cases=[custom])
    store.configure_prompt(10, True, 'gpt-6-sol', s.DEFAULT_PROMPT, True, 3)
    store.configure_prompt(11, False, 'gpt-6-sol', s.DEFAULT_PROMPT, test_mode='qa', cases=[original])
    with store.db() as db:
        db.execute("UPDATE prompt_monitors SET pause_status='paused',paused_at=123 WHERE account_id=11")
        before_upgrade = existing_state(db)
    fresh = s.Store(store.path)
    state = fresh.get_prompt(7)
    assert json.loads(state['cases'])[0]['prompt'] == s.DEFAULT_QA_CASES[0]['prompt']
    assert state['auto_pause_threshold'] == 1 and state['failure_streak'] == 0 and state['pause_status'] == ''
    assert fresh.public(7)['prompt_test']['history'] == history
    assert not fresh.is_prompt_current(job)
    assert fresh.public(8)['prompt_test']['cases'][0]['prompt'] == '自定义糖果题'
    assert fresh.public(9)['prompt_test']['cases'][0]['expected_answer'] == '29'
    assert fresh.get_prompt(10)['auto_pause_threshold'] == 3
    assert fresh.get_prompt(11)['pause_status'] == 'paused' and not fresh.get_prompt(11)['enabled']
    with fresh.db() as db:
        assert state_preserved(db, before_upgrade)
        db.execute('UPDATE prompt_monitors SET enabled=0 WHERE account_id=7')
        assert not state_preserved(db, before_upgrade)
        db.execute('UPDATE prompt_monitors SET enabled=1 WHERE account_id=7')
    assert s.Store(store.path).get_prompt(7) == state


def test_failed_manual_pause_retries_without_duplicate_calls_or_stale_review_rearming(store):
    result_id, control = manual_round(store)
    calls = []
    def fail(identity):
        calls.append(identity)
        raise s.ProbeError('pause unavailable')
    control.pause = fail
    client = s.create_app(store, FakeAccounts(), lambda _: True, control).test_client()
    url = f'/accounts/7/prompt-test/results/{result_id}/reviews/drawing'
    assert client.put(url, json={'status': 'red'}).status_code == 200
    assert store.get_prompt(7)['pause_status'] == 'failed'
    client.put(url, json={'status': 'red'})
    assert calls == [7]  # Retry deadline is preserved across repeated scoring.
    client.put(url, json={'status': 'green'})
    store.apply_pending_prompt_pause(FakeAccounts(), control)
    assert calls == [7] and store.get_prompt(7)['pause_status'] == ''
