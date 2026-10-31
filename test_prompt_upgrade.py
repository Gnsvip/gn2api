import sqlite3

import pytest

from upgrade_prompt_test import running_count, updated_nginx, VERSION, existing_state, state_preserved, disable_qa_on_rollback


def test_only_modeltrace_script_and_location_change():
    source = '''server {
    client_max_body_size 256m;
    sub_filter '</body>' '<script src="/custom/modeltrace.js?v=some-existing-version"></script></body>';
    location ^~ /custom/modeltrace/api/ {
        proxy_pass http://127.0.0.1:17861/;
        client_max_body_size 4k;
    }
    location /other { client_max_body_size 50m; }
}'''
    result = updated_nginx(source)
    assert result == source.replace('some-existing-version', VERSION).replace('client_max_body_size 4k;', 'client_max_body_size 128k;')
    assert updated_nginx(result) == result
    for broken in [source.replace('modeltrace.js?v=', 'absent.js?v='), source + source,
                   source.replace('client_max_body_size 4k;', '')]:
        with pytest.raises(RuntimeError):
            updated_nginx(broken)


def test_upgrade_waits_for_both_kinds_of_monitor():
    with sqlite3.connect(':memory:') as db:
        db.execute('CREATE TABLE monitors(running INTEGER)')
        db.execute('INSERT INTO monitors VALUES(0)')
        assert running_count(db) == 0
        db.execute('CREATE TABLE prompt_monitors(running INTEGER)')
        db.execute('INSERT INTO prompt_monitors VALUES(1)')
        assert running_count(db) == 1


def test_prompt_upgrade_retains_previous_columns_and_question_history():
    with sqlite3.connect(':memory:') as db:
        db.execute('CREATE TABLE prompt_monitors(account_id INTEGER, enabled INTEGER, prompt TEXT)')
        db.execute("INSERT INTO prompt_monitors VALUES(7,1,'{{challenge}}')")
        db.execute('CREATE TABLE prompt_results(id INTEGER, result TEXT)')
        db.execute("INSERT INTO prompt_results VALUES(1,'original history')")
        before = existing_state(db)
        db.execute("ALTER TABLE prompt_monitors ADD COLUMN test_mode TEXT DEFAULT 'attribution'")
        assert state_preserved(db, before)
        db.execute('UPDATE prompt_monitors SET enabled=0')
        assert not state_preserved(db, before)


def test_prompt_rollback_cannot_turn_qa_into_numeric_probes():
    with sqlite3.connect(':memory:') as db:
        db.execute('CREATE TABLE prompt_monitors(account_id INTEGER, enabled INTEGER, running INTEGER, generation INTEGER, test_mode TEXT)')
        db.executemany('INSERT INTO prompt_monitors VALUES(?,1,0,1,?)', [(7, 'qa'), (8, 'attribution')])
        disable_qa_on_rollback(db)
        assert db.execute('SELECT account_id,enabled,generation FROM prompt_monitors ORDER BY account_id').fetchall() == [(7, 0, 2), (8, 1, 1)]
