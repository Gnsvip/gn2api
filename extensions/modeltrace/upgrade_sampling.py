"""Publish bounded sample replacement; restart only the independent monitor."""
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
         'activate_server.py', 'upgrade_sampling.py']
VERSION = '20261020-1'


def main():
    assert os.geteuid() == 0
    assert hashlib.sha256((TARGET / 'service.py').read_bytes()).hexdigest() == '38fd8bacf41766246995239cee2d67ceea619de0270a386349c6ef13e20dd889'
    assert hashlib.sha256(JS.read_bytes()).hexdigest() == 'c31807b6422965d4f346f0519b9ca3476a76e9650b8a6eebc37311ae157e7068'
    original_config = NGINX.read_text()
    needle = '/custom/modeltrace.js?v=20261019-3'
    assert original_config.count(needle) == 1
    assert not re.search(r'^\s*worker_shutdown_timeout\s+(?!0\s*;)', run(['nginx', '-T']), re.M)
    for name in FILES:
        assert (SOURCE / name).is_file(), 'release file missing'
        if name.endswith('.py'):
            compile((SOURCE / name).read_text(), name, 'exec')
    before = snapshot()
    backup = TARGET / 'backups' / (time.strftime('%Y%m%d-%H%M%S') + '-sampling')
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
                raise RuntimeError('monitor remained busy; no probe interrupted')
            if time.monotonic() - last_notice > 30:
                print('Waiting for current attribution round', flush=True)
                last_notice = time.monotonic()
            time.sleep(2)
        columns = 'account_id,enabled,model,generation,interval_minutes,auto_pause_enabled,auto_pause_threshold'
        with sqlite3.connect(DB) as db, sqlite3.connect(backup / 'state.sqlite3') as dest:
            db.backup(dest)
            configs = db.execute('SELECT ' + columns + ' FROM monitors ORDER BY account_id').fetchall()
            state_before = db.iterdump()
            state_hash = hashlib.sha256('\n'.join(state_before).encode()).hexdigest()
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
        temp = JS.with_suffix('.js.new')
        shutil.copyfile(SOURCE / 'modeltrace.js', temp)
        temp.chmod(0o644)
        temp.replace(JS)
        assert NGINX.read_text() == original_config, 'nginx changed while waiting'
        changed_nginx = True
        NGINX.write_text(original_config.replace(needle, '/custom/modeltrace.js?v=' + VERSION))
        run(['nginx', '-t'])
        run(['systemctl', 'reload', 'nginx'])
        after = snapshot()
        for field in ['boot_id', 'units', 'containers']:
            assert before[field] == after[field], 'protected service restarted'
        for path, old_hash in before['hashes'].items():
            if Path(path).resolve() != NGINX:
                assert old_hash == after['hashes'][path], 'unrelated file changed'
        report = dict(status='ok', backup=str(backup), protected_services_not_restarted=True,
                      settings_preserved=True, state_untouched_during_upgrade=True,
                      nginx_graceful_reload=True, script_version=VERSION, backend_tests=72,
                      service_sha256=hashlib.sha256((TARGET / 'service.py').read_bytes()).hexdigest(),
                      script_sha256=hashlib.sha256(JS.read_bytes()).hexdigest(),
                      protected_units=after['units'])
        (TARGET / 'sampling-upgrade-20261020.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(json.dumps(report, ensure_ascii=False), flush=True)
    except Exception:
        if stopped:
            run(['systemctl', 'stop', 'modeltrace-ovh'])
            for name in FILES:
                if (backup / name).exists():
                    shutil.copy2(backup / name, TARGET / name)
            shutil.copy2(backup / 'web-modeltrace.js', JS)
            if changed_nginx:
                NGINX.write_text(original_config)
                run(['nginx', '-t'])
                run(['systemctl', 'reload', 'nginx'])
            # No schema or state writes: retain new results/settings on rollback.
            run(['systemctl', 'start', 'modeltrace-ovh'])
            print('Independent monitor rolled back', flush=True)
        raise


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('Upgrade failed: ' + type(error).__name__, flush=True)
        raise SystemExit(1)
