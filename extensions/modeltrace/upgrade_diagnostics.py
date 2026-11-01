"""Upgrade failure descriptions only; no schema, scheduling, or nginx changes."""
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import time

sys.path.insert(0, '/opt/modeltrace-ovh')
from upgrade_auto_pause import snapshot, run, get_json

SOURCE = Path(__file__).resolve().parent
TARGET = Path('/opt/modeltrace-ovh')
DB = Path('/var/lib/modeltrace-ovh/state.sqlite3')
FILES = ['service.py', 'test_service.py', 'README.md', 'upgrade_diagnostics.py']


def main():
    assert hashlib.sha256((TARGET / 'service.py').read_bytes()).hexdigest() == 'b91aeb23ddcbdc1f63388ee499f789514c3c2146f9df38d3b1ae2996883ab22a'
    compile((SOURCE / 'service.py').read_text(), 'service.py', 'exec')
    before = snapshot()
    backup = TARGET / 'backups' / (time.strftime('%Y%m%d-%H%M%S') + '-diagnostics')
    backup.mkdir(mode=0o700)
    (backup / 'baseline.json').write_text(json.dumps(before, indent=2))
    for name in FILES:
        if (TARGET / name).exists():
            shutil.copy2(TARGET / name, backup / name)
    stopped = False
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
                raise RuntimeError('monitor remained busy; no probe interrupted')
            if time.monotonic() - last_notice > 30:
                print('Waiting for current attribution round', flush=True)
                last_notice = time.monotonic()
            time.sleep(2)
        columns = 'account_id,enabled,model,generation,interval_minutes,auto_pause_enabled,auto_pause_threshold'
        with sqlite3.connect(DB) as db, sqlite3.connect(backup / 'state.sqlite3') as dest:
            db.backup(dest)
            configs = db.execute('SELECT ' + columns + ' FROM monitors ORDER BY account_id').fetchall()
        for name in FILES:
            temp = TARGET / (name + '.new')
            shutil.copyfile(SOURCE / name, temp)
            temp.chmod(0o644)
            temp.replace(TARGET / name)
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
        assert before == snapshot(), 'protected services or files changed'
        report = dict(status='ok', backup=str(backup), protected_services_and_files_unchanged=True,
                      settings_preserved=True, backend_tests=59,
                      service_sha256=hashlib.sha256((TARGET / 'service.py').read_bytes()).hexdigest())
        (TARGET / 'diagnostics-upgrade-20261019.json').write_text(json.dumps(report, indent=2))
        print(json.dumps(report), flush=True)
    except Exception:
        if stopped:
            run(['systemctl', 'stop', 'modeltrace-ovh'])
            for name in FILES:
                if (backup / name).exists():
                    shutil.copy2(backup / name, TARGET / name)
            run(['systemctl', 'start', 'modeltrace-ovh'])
            print('Independent monitor rolled back', flush=True)
        raise


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('Upgrade failed: ' + type(error).__name__, flush=True)
        raise SystemExit(1)
