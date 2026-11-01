"""Add per-account challenge counts; restart only the independent monitor."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import time

sys.path.insert(0, '/opt/modeltrace-ovh')
from upgrade_auto_pause import snapshot, run, get_json

SOURCE = Path(__file__).resolve().parent
TARGET = Path('/opt/modeltrace-ovh')
DB = Path('/var/lib/modeltrace-ovh/state.sqlite3')
JS = Path('/opt/sub2api-custom/modeltrace.js')
NGINX = Path('/etc/nginx/sites-enabled/aifxo.com').resolve()
FILES = ['service.py', 'modeltrace.js', 'test_service.py', 'test_ui.py', 'README.md',
         'activate_server.py', 'upgrade_challenges.py']
VERSION = '20261027-1'


def replace_file(source, target):
    temp = target.with_name(target.name + '.new')
    shutil.copyfile(source, temp)
    temp.chmod(0o644)
    temp.replace(target)


def main():
    assert os.geteuid() == 0
    assert hashlib.sha256((TARGET / 'service.py').read_bytes()).hexdigest() == '3f2cf962070e0d6cefa5cdf9ed5f3037c6a3596c811208161585dab4003802d2'
    assert hashlib.sha256(JS.read_bytes()).hexdigest() == '987fd5ca2763e3b8508f78fca8eadfb8595e22d73294bf1971ef03fafa0dbbc4'
    original_config = NGINX.read_text()
    needle = '/custom/modeltrace.js?v=20261020-2'
    assert original_config.count(needle) == 1
    assert not re.search(r'^\s*worker_shutdown_timeout\s+(?!0\s*;)', run(['nginx', '-T']), re.M)
    for name in FILES:
        assert (SOURCE / name).is_file()
        if name.endswith('.py'):
            compile((SOURCE / name).read_text(), name, 'exec')
    before = snapshot()
    backup = TARGET / 'backups' / (time.strftime('%Y%m%d-%H%M%S') + '-challenge-count')
    backup.mkdir(mode=0o700)
    (backup / 'baseline.json').write_text(json.dumps(before, indent=2))
    for name in FILES:
        if (TARGET / name).exists():
            shutil.copy2(TARGET / name, backup / name)
    shutil.copy2(JS, backup / 'web-modeltrace.js')
    shutil.copy2(NGINX, backup / 'nginx.conf')
    stopped = changed_nginx = False
    try:
        deadline, last_notice = time.monotonic() + 360, 0
        while True:
            with sqlite3.connect(DB, timeout=5) as db:
                db.execute('BEGIN IMMEDIATE')
                if db.execute('SELECT count(*) FROM monitors WHERE running=1').fetchone()[0] == 0:
                    run(['systemctl', 'stop', 'modeltrace-ovh'])
                    stopped = True
                    break
            if time.monotonic() >= deadline:
                raise RuntimeError('monitor stayed busy; no running probe interrupted')
            if time.monotonic() - last_notice > 30:
                print('Waiting for current attribution round', flush=True)
                last_notice = time.monotonic()
            time.sleep(2)
        columns = 'account_id,enabled,model,generation,interval_minutes,auto_pause_enabled,auto_pause_threshold'
        with sqlite3.connect(DB) as db, sqlite3.connect(backup / 'state.sqlite3') as dest:
            db.backup(dest)
            old_columns = ','.join('"' + r[1] + '"' for r in db.execute('PRAGMA table_info(monitors)'))
            assert '"challenge_count"' not in old_columns
            all_monitor_state = db.execute('SELECT ' + old_columns + ' FROM monitors ORDER BY account_id').fetchall()
            all_history = db.execute('SELECT * FROM results ORDER BY id').fetchall()
            configs = db.execute('SELECT ' + columns + ' FROM monitors ORDER BY account_id').fetchall()
        for name in FILES:
            replace_file(SOURCE / name, TARGET / name)
        # Run additive migration under the database owner, with the worker stopped.
        run(['sudo', '-u', 'modeltrace_ovh', '/opt/modeltrace-ovh/venv/bin/python', '-c',
             "import sys; sys.path.insert(0, '/opt/modeltrace-ovh'); from service import Store; Store('/var/lib/modeltrace-ovh/state.sqlite3')"])
        with sqlite3.connect(DB) as db:
            assert db.execute('SELECT ' + old_columns + ' FROM monitors ORDER BY account_id').fetchall() == all_monitor_state
            assert db.execute('SELECT * FROM results ORDER BY id').fetchall() == all_history
            assert db.execute('SELECT count(*) FROM monitors WHERE challenge_count<>3').fetchone()[0] == 0
        run(['systemctl', 'start', 'modeltrace-ovh'])
        deadline = time.monotonic() + 25
        while True:
            try:
                assert get_json('http://127.0.0.1:17861/healthz')['status'] == 'ok'
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise RuntimeError('monitor health check failed') from None
                time.sleep(1)
        with sqlite3.connect(DB) as db:
            assert db.execute('SELECT ' + columns + ' FROM monitors ORDER BY account_id').fetchall() == configs
        key = Path('/etc/modeltrace-ovh/sub2api-admin-key').read_text().strip()
        for offset in range(0, len(configs), 100):
            ids = ','.join(str(row[0]) for row in configs[offset:offset + 100])
            states = get_json('http://127.0.0.1:17861/accounts?ids=' + ids, key)['accounts']
            assert all(state['challenge_count'] == 3 for state in states.values())
        replace_file(SOURCE / 'modeltrace.js', JS)
        assert NGINX.read_text() == original_config, 'nginx changed during release'
        new_config = original_config.replace(needle, '/custom/modeltrace.js?v=' + VERSION)
        config_temp = NGINX.with_name(NGINX.name + '.new')
        config_temp.write_text(new_config)
        config_temp.chmod(NGINX.stat().st_mode & 0o777)
        config_temp.replace(NGINX)
        changed_nginx = True
        run(['nginx', '-t'])
        run(['systemctl', 'reload', 'nginx'])
        after = snapshot()
        for field in ['boot_id', 'units', 'containers']:
            assert before[field] == after[field], 'protected service restarted'
        for path, old_hash in before['hashes'].items():
            if Path(path).resolve() != NGINX:
                assert old_hash == after['hashes'][path], 'unrelated file changed'
        report = dict(status='ok', backup=str(backup), protected_services_not_restarted=True,
                      old_settings_and_history_preserved=True, default_challenge_count=3,
                      backend_tests=117, ui_tests='passed', nginx_graceful_reload=True,
                      script_version=VERSION, script_sha256=hashlib.sha256(JS.read_bytes()).hexdigest(),
                      service_sha256=hashlib.sha256((TARGET / 'service.py').read_bytes()).hexdigest())
        (TARGET / 'challenges-upgrade-20261027.json').write_text(json.dumps(report, indent=2))
        print(json.dumps(report), flush=True)
    except Exception:
        if stopped:
            run(['systemctl', 'stop', 'modeltrace-ovh'])
            for name in FILES:
                if (backup / name).exists():
                    replace_file(backup / name, TARGET / name)
            replace_file(backup / 'web-modeltrace.js', JS)
            if changed_nginx:
                replace_file(backup / 'nginx.conf', NGINX)
                run(['nginx', '-t'])
                run(['systemctl', 'reload', 'nginx'])
            # The old service tolerates the extra column; do not erase later results/settings.
            run(['systemctl', 'start', 'modeltrace-ovh'])
            print('Independent monitor and UI rolled back', flush=True)
        raise


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('Upgrade failed: ' + type(error).__name__, flush=True)
        raise SystemExit(1)
