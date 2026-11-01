"""Upgrade the independent monitor while preserving running business requests."""
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
FILES = ['service.py', 'modeltrace.js', 'test_service.py', 'test_ui.py', 'README.md', 'upgrade_interval.py']


def main():
    assert os.geteuid() == 0
    assert hashlib.sha256((TARGET / 'service.py').read_bytes()).hexdigest() == 'f945a4d4c1d71881fe15b295e40317440bdf46d0f2da89fd53ce1af538f3dcc4'
    original_config = NGINX.read_text()
    needle = '/custom/modeltrace.js?v=20261019-2'
    assert original_config.count(needle) == 1
    assert not re.search(r'^\s*worker_shutdown_timeout\s+(?!0\s*;)', run(['nginx', '-T']), re.M)
    compile((SOURCE / 'service.py').read_text(), 'service.py', 'exec')
    before = snapshot()
    key = Path('/etc/modeltrace-ovh/sub2api-admin-key').read_text().strip()
    assert get_json('http://127.0.0.1:8080/api/v1/admin/system/version', key)['code'] == 0
    backup = TARGET / 'backups' / (time.strftime('%Y%m%d-%H%M%S') + '-interval-credits')
    backup.mkdir(mode=0o700)
    (backup / 'baseline.json').write_text(json.dumps(before, indent=2))
    for name in FILES:
        if (TARGET / name).exists():
            shutil.copy2(TARGET / name, backup / name)
    shutil.copy2(JS, backup / 'web-modeltrace.js')
    shutil.copy2(NGINX, backup / 'nginx.conf')
    stopped = changed_nginx = False
    try:
        deadline = time.monotonic() + 600
        last_notice = 0
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
        with sqlite3.connect(DB) as db, sqlite3.connect(backup / 'state.sqlite3') as dest:
            db.backup(dest)
            columns = 'account_id,enabled,model,generation,auto_pause_enabled,auto_pause_threshold,mismatch_streak,pause_status'
            configs = db.execute('SELECT ' + columns + ' FROM monitors ORDER BY account_id').fetchall()
            # Requeue only the account the user reported, if it is still blocked by the removed guard.
            requeued = db.execute("""UPDATE monitors SET next_run=?,message='等待重新检测，已移除百分比限制'
                WHERE account_id=12203 AND enabled=1 AND pause_status='' AND message=?""",
                (time.time() + 2, '账号额度使用已达 90%，优先保留业务额度')).rowcount
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
            assert db.execute('SELECT count(*) FROM monitors WHERE interval_minutes<>10').fetchone()[0] == 0
        temp = JS.with_suffix('.js.new')
        shutil.copyfile(SOURCE / 'modeltrace.js', temp)
        temp.chmod(0o644)
        temp.replace(JS)
        NGINX.write_text(original_config.replace(needle, '/custom/modeltrace.js?v=20261019-3'))
        changed_nginx = True
        run(['nginx', '-t'])
        run(['systemctl', 'reload', 'nginx'])
        after = snapshot()
        for field in ['boot_id', 'units', 'containers']:
            assert before[field] == after[field], 'protected service restarted'
        for path, old_hash in before['hashes'].items():
            if Path(path).resolve() != NGINX:
                assert old_hash == after['hashes'][path], 'unrelated file changed'
        ids = ','.join(str(row[0]) for row in configs)
        states = get_json('http://127.0.0.1:17861/accounts?ids=' + ids, key)['accounts']
        report = dict(status='ok', backup=str(backup), protected_services_not_restarted=True,
                      nginx_graceful_reload=True, script_version='20261019-3', requeued_12203=bool(requeued),
                      monitors=[{k:s[k] for k in ['account_id','enabled','model','interval_minutes','auto_pause_enabled','auto_pause_threshold','schedulable','running']} for s in states.values()],
                      service_sha256=hashlib.sha256((TARGET / 'service.py').read_bytes()).hexdigest())
        (TARGET / 'interval-credits-upgrade-20261019.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(json.dumps(report, ensure_ascii=False), flush=True)
    except Exception:
        if stopped:
            run(['systemctl', 'stop', 'modeltrace-ovh'])
            for name in FILES:
                if (backup / name).exists():
                    shutil.copy2(backup / name, TARGET / name)
            shutil.copy2(backup / 'web-modeltrace.js', JS)
            if (backup / 'state.sqlite3').exists():
                with sqlite3.connect(backup / 'state.sqlite3') as source, sqlite3.connect(DB) as db:
                    source.backup(db)
            if changed_nginx:
                NGINX.write_text(original_config)
                run(['nginx', '-t'])
                run(['systemctl', 'reload', 'nginx'])
            run(['systemctl', 'start', 'modeltrace-ovh'])
            print('Independent monitor rolled back', flush=True)
        raise


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('Upgrade failed: ' + type(error).__name__, flush=True)
        raise SystemExit(1)
