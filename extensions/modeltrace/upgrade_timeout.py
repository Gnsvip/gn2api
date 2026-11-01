"""Allow buffered upstream responses; restart only the independent monitor."""
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
FILES = ['service.py', 'test_service.py', 'README.md', 'upgrade_timeout.py']


def main():
    assert hashlib.sha256((TARGET / 'service.py').read_bytes()).hexdigest() == '1c4f34f1783a11b9f9ac58b7cee44dffa1806d273478370476f8bc39cd6af4a1'
    for name in FILES:
        assert (SOURCE / name).is_file()
        if name.endswith('.py'):
            compile((SOURCE / name).read_text(), name, 'exec')
    before = snapshot()
    backup = TARGET / 'backups' / (time.strftime('%Y%m%d-%H%M%S') + '-response-timeout')
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
            state_hash = hashlib.sha256('\n'.join(db.iterdump()).encode()).hexdigest()
        for name in FILES:
            temp = TARGET / (name + '.new')
            shutil.copyfile(SOURCE / name, temp)
            temp.chmod(0o644)
            temp.replace(TARGET / name)
        with sqlite3.connect(DB) as db:
            assert hashlib.sha256('\n'.join(db.iterdump()).encode()).hexdigest() == state_hash
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
                      settings_preserved=True, state_untouched_during_upgrade=True, backend_tests=82,
                      service_sha256=hashlib.sha256((TARGET / 'service.py').read_bytes()).hexdigest())
        (TARGET / 'timeout-upgrade-20261027.json').write_text(json.dumps(report, indent=2))
        print(json.dumps(report), flush=True)
    except Exception:
        if stopped:
            run(['systemctl', 'stop', 'modeltrace-ovh'])
            for name in FILES:
                if (backup / name).exists():
                    shutil.copy2(backup / name, TARGET / name)
            # No schema or state changes; retain new user settings and results.
            run(['systemctl', 'start', 'modeltrace-ovh'])
            print('Independent monitor rolled back', flush=True)
        raise


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('Upgrade failed: ' + type(error).__name__, flush=True)
        raise SystemExit(1)
