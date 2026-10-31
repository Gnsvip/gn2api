import sqlite3

import pytest

from upgrade_prompt_test import running_count, updated_nginx, VERSION


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
    assert result == source.replace('some-existing-version', VERSION).replace('client_max_body_size 4k;', 'client_max_body_size 16k;')
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
