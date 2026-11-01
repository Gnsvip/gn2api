"""Opt-in scheduling decisions and deleted-account retention, using local fixtures."""
import json
import sqlite3

import pytest

import service as s
from test_service import FakeAccounts, PromptControl, claim, prompt_claim, store


@pytest.mark.parametrize('prompt', [False, True])
def test_three_unverified_candidate_mismatches_pause_exactly_on_third_round(store, monkeypatch, prompt):
    monkeypatch.setattr(s, 'analyze_global_outputs', lambda outputs, bank: {
        'results': [{'model': 'gpt-5.6-luna', 'probability': .9999},
                    {'model': 'gpt-6-astra', 'probability': .0001}],
        'prediction': 'gpt-5.6-luna', 'probability': .9999, 'used_outputs': len(outputs)})
    if prompt:
        store.configure_prompt(7, True, 'gpt-6-astra', '{{challenge}}', True, 3, challenge_count=1)
    else:
        store.configure(7, True, 'gpt-6-astra', True, 3, challenge_count=1)
    store.configure(8, False, 'gpt-6-astra')
    control, accounts = PromptControl(), FakeAccounts()
    for count in (1, 2, 3):
        job = prompt_claim(store) if prompt else claim(store)
        result = s.run_prompt_round({**job, 'test_mode': 'attribution', 'prompt': '{{challenge}}'},
                                   store, accounts, control, attribution_monitor=not prompt)
        assert result['status'] == 'red' and result['requests'] == 1
        assert result['bank_applicability'] == 'unverified' and not result['confident']
        (store.finish_prompt if prompt else store.finish)(job, result)
        # A restart must preserve a pending threshold decision, not cancel it for calibration.
        fresh = s.Store(store.path)
        (fresh.apply_pending_prompt_pause if prompt else fresh.apply_pending_pause)(accounts, control)
        assert control.calls == ([7] if count == 3 else [])
        state = fresh.get_prompt(7) if prompt else fresh.get(7)
        assert state['failure_streak' if prompt else 'mismatch_streak'] == count
    assert state['pause_status'] == 'paused' and not state['enabled']
    assert len(control.prompts) == 3
    assert store.get(8)['pause_status'] == ''


TABLES = ('monitors', 'prompt_monitors', 'results', 'prompt_results', 'prompt_artifacts',
          'probe_attempts', 'probe_attempt_events', 'probe_evidence')


def seed_account(store, identity):
    store.configure(identity, False, 'gpt-6-astra')
    store.configure_prompt(identity, False, 'gpt-6-astra', '{{challenge}}')
    with store.db() as db:
        db.execute('INSERT INTO results(account_id,checked_at,model,result) VALUES(?,0,?,?)',
                   (identity, 'gpt-6-astra', '{}'))
        result_id = db.execute('INSERT INTO prompt_results(account_id,checked_at,model,result) VALUES(?,0,?,?)',
                              (identity, 'gpt-6-astra', '{}')).lastrowid
        db.execute('INSERT INTO prompt_artifacts VALUES(?,?,?,?)',
                   (result_id, 'draw', '<svg/>', 'saved prompt'))
        db.execute('INSERT INTO probe_attempts(attempt_id,run_id,account_id,started_at,metadata) VALUES(?,?,?,0,?)',
                   (str(identity), 'run', identity, '{}'))
        db.execute('INSERT INTO probe_attempt_events(attempt_id,at,kind,payload) VALUES(?,0,?,?)',
                   (str(identity), 'diagnostic', json.dumps({'output': 'saved original'})))
        db.execute('INSERT INTO probe_evidence(account_id,condition_sha256,slot,created_at,run_id,summary) '
                   'VALUES(?,?,?,0,?,?)', (identity, 'condition', 'anomaly', 'run', '{}'))


def snapshot(store):
    with store.db() as db:
        return {t: [tuple(r) for r in db.execute('SELECT * FROM ' + t + ' ORDER BY rowid')] for t in TABLES}


class DeletedAccounts(FakeAccounts):
    def existing_ids(self, ids):
        return set(ids) - {7}


def test_deleted_account_removes_all_associated_data_but_keeps_live_account(store):
    seed_account(store, 8)
    live = snapshot(store)
    seed_account(store, 7)
    assert store.cleanup_deleted_accounts(DeletedAccounts()) == 1
    assert snapshot(store) == live
    assert s.Store(store.path).cleanup_deleted_accounts(DeletedAccounts()) == 0


def test_failed_account_lookup_never_purges_history(store):
    seed_account(store, 7)
    before = snapshot(store)
    class Unreachable(FakeAccounts):
        def existing_ids(self, ids):
            raise ConnectionError('database unavailable')
    with pytest.raises(ConnectionError):
        store.cleanup_deleted_accounts(Unreachable())
    assert snapshot(store) == before


def test_cleanup_rolls_back_every_table_if_any_delete_fails(store):
    seed_account(store, 7)
    before = snapshot(store)
    with store.db() as db:
        db.execute("CREATE TRIGGER fixture_block BEFORE DELETE ON prompt_results "
                   "BEGIN SELECT RAISE(ABORT, 'fixture failure'); END")
    with pytest.raises(sqlite3.IntegrityError):
        store.cleanup_deleted_accounts(DeletedAccounts())
    assert snapshot(store) == before


def test_late_round_cannot_restore_deleted_account_history(store):
    seed_account(store, 7)
    store.configure(7, True, 'gpt-6-astra')
    job = claim(store)
    assert store.cleanup_deleted_accounts(DeletedAccounts()) == 1
    store.finish(job, {'status': 'red', '_context': 'obsolete'})
    assert all(not rows for rows in snapshot(store).values())


def test_existence_lookup_uses_deletion_not_status_or_schedulability(monkeypatch):
    accounts = s.Accounts('fixture')
    calls = []
    def query(sql, params):
        calls.append((sql, params))
        return [{'id': 8}]
    monkeypatch.setattr(accounts, 'query', query)
    assert accounts.existing_ids([7, 8]) == {8}
    assert calls == [('SELECT id FROM accounts WHERE id=ANY(%s) AND deleted_at IS NULL', ([7, 8],))]
    assert accounts.existing_ids([]) == set() and len(calls) == 1


def test_cleanup_includes_accounts_with_only_attempts_left(store):
    seed_account(store, 7)
    with store.db() as db:
        for t in ('monitors', 'prompt_monitors', 'results', 'prompt_results', 'prompt_artifacts', 'probe_evidence'):
            db.execute('DELETE FROM ' + t)
    assert store.cleanup_deleted_accounts(DeletedAccounts()) == 1
    assert all(not rows for rows in snapshot(store).values())


def test_scheduler_cleans_deleted_accounts_before_dispatching_jobs(store, monkeypatch):
    seed_account(store, 7)
    store.configure(7, True, 'gpt-6-astra')
    with store.db() as db:
        db.execute('UPDATE monitors SET next_run=0')
    monkeypatch.setattr(s, 'run_prompt_round', lambda *a, **kw: pytest.fail('deleted account must not be probed'))
    class StopAfterOne:
        waits = 0
        def wait(self, seconds):
            self.waits += 1
            return self.waits > 1
    s.scheduler(store, DeletedAccounts(), StopAfterOne(), '', PromptControl())
    assert all(not rows for rows in snapshot(store).values())
