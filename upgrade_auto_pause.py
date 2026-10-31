"""One-time guarded upgrade. Restarts only the independent ModelTrace service."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import time
import urllib.request

SOURCE = Path(__file__).resolve().parent
TARGET = Path('/opt/modeltrace-ovh')
DB = Path('/var/lib/modeltrace-ovh/state.sqlite3')
JS = Path('/opt/sub2api-custom/modeltrace.js')
CREDENTIAL = Path('/etc/modeltrace-ovh/sub2api-admin-key')
DROPIN = Path('/etc/systemd/system/modeltrace-ovh.service.d/auto-pause.conf')
EXPECTED = '6efbe1c718d6c1d9a7d320aea869bd9655b2dbb647f4c01622445b77fde3affc'
FILES = ['service.py', 'modeltrace.js', 'test_service.py', 'test_ui.py', 'README.md', 'auto-pause.conf', 'upgrade_auto_pause.py']


def run(args):
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=30).stdout.strip()


def snapshot():
    units = ['sub2api', 'nginx', 'postgresql', 'docker', 'xray-ovh', 'hysteria-ovh']
    paths = ['/opt/sub2api/sub2api', '/etc/nginx/nginx.conf', '/etc/nginx/sites-enabled/aifxo.com',
             '/etc/nginx/sites-enabled/api.aifxo.com', '/etc/nginx/sites-enabled/cpa.aifxo.com']
    ids = run(['docker', 'ps', '-q']).splitlines()
    return dict(boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                units={u: run(['systemctl', 'show', u, '-p', 'MainPID', '-p', 'ExecMainStartTimestamp']) for u in units},
                hashes={p: hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in paths if Path(p).is_file()},
                containers=run(['docker', 'inspect', '--format', '{{.Id}} {{.State.StartedAt}}', *ids]) if ids else '')


def get_json(url, key=None):
    request = urllib.request.Request(url, headers={'X-API-Key': key} if key else {})
    with urllib.request.urlopen(request, timeout=8) as response:
        return json.load(response)


def main():
    if os.geteuid() != 0:
        raise RuntimeError('root required')
    if hashlib.sha256((TARGET / 'service.py').read_bytes()).hexdigest() != EXPECTED:
        raise RuntimeError('existing service changed; inspect before upgrading')
    if CREDENTIAL.exists() or DROPIN.exists():
        raise RuntimeError('credential/drop-in already exists; inspect before upgrading')
    for name in FILES:
        if not (SOURCE / name).is_file():
            raise RuntimeError('incomplete release')
    compile((SOURCE / 'service.py').read_text(), 'service.py', 'exec')
    before = snapshot()
    key = run(['sudo', '-u', 'postgres', 'psql', '-X', '-d', 'sub2api', '-At', '-c',
               "SELECT value FROM settings WHERE key='admin_api_key'"])
    if not key or get_json('http://127.0.0.1:8080/api/v1/admin/system/version', key).get('code') != 0:
        raise RuntimeError('admin credential validation failed')
    backup = TARGET / 'backups' / (time.strftime('%Y%m%d-%H%M%S') + '-auto-pause')
    backup.mkdir(mode=0o700)
    (backup / 'baseline.json').write_text(json.dumps(before, indent=2))
    existing = {}
    for name in FILES:
        existing[name] = (TARGET / name).exists()
        if existing[name]:
            shutil.copy2(TARGET / name, backup / name)
    shutil.copy2(JS, backup / 'web-modeltrace.js')
    stopped = False
    try:
        # Hold the SQLite writer lock between idle check and service stop, preventing a new claim.
        deadline = time.monotonic() + 360
        while True:
            with sqlite3.connect(DB, timeout=5) as db:
                db.execute('BEGIN IMMEDIATE')
                if db.execute('SELECT count(*) FROM monitors WHERE running=1').fetchone()[0] == 0:
                    run(['systemctl', 'stop', 'modeltrace-ovh'])
                    stopped = True
                    break
            if time.monotonic() >= deadline:
                raise RuntimeError('monitor stayed busy; no running detection interrupted')
            print('Waiting for active attribution round to finish', flush=True)
            time.sleep(10)
        with sqlite3.connect(DB) as db, sqlite3.connect(backup / 'state.sqlite3') as copy:
            db.backup(copy)
            configs = db.execute('SELECT account_id,enabled,model,next_run FROM monitors').fetchall()
        CREDENTIAL.parent.mkdir(mode=0o700, exist_ok=True)
        fd = os.open(CREDENTIAL, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as file:
            file.write(key + '\n')
        DROPIN.parent.mkdir(mode=0o755, exist_ok=True)
        shutil.copyfile(SOURCE / 'auto-pause.conf', DROPIN)
        DROPIN.chmod(0o644)
        for name in FILES:
            temp = TARGET / (name + '.new')
            shutil.copyfile(SOURCE / name, temp)
            temp.chmod(0o644)
            temp.replace(TARGET / name)
        run(['systemctl', 'daemon-reload'])
        run(['systemctl', 'start', 'modeltrace-ovh'])
        deadline = time.monotonic() + 25
        while True:
            try:
                assert get_json('http://127.0.0.1:17861/healthz')['status'] == 'ok'
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise RuntimeError('new monitor failed health check') from None
                time.sleep(1)
        with sqlite3.connect(DB) as db:
            new_configs = db.execute('SELECT account_id,enabled,model FROM monitors').fetchall()
            assert new_configs == [tuple(row[:3]) for row in configs]
            assert db.execute('SELECT count(*) FROM monitors WHERE auto_pause_enabled<>0 OR auto_pause_threshold<>3 OR mismatch_streak<>0').fetchone()[0] == 0
        ids = ','.join(str(row[0]) for row in configs)
        states = get_json('http://127.0.0.1:17861/accounts?ids=' + ids, key)['accounts'] if ids else {}
        assert all(not state['auto_pause_enabled'] and state['auto_pause_threshold'] == 3 for state in states.values())
        # Only publish the new UI after the backend accepts the new settings.
        temp = JS.with_suffix('.js.new')
        shutil.copyfile(SOURCE / 'modeltrace.js', temp)
        temp.chmod(0o644)
        temp.replace(JS)
        after = snapshot()
        assert before == after, 'protected service or configuration changed'
        report = dict(status='ok', backup=str(backup), protected_services_unchanged=True,
                      service_sha256=hashlib.sha256((TARGET / 'service.py').read_bytes()).hexdigest(),
                      auto_pause_default=False, threshold=3,
                      monitors=[dict(account_id=int(i), enabled=s['enabled'], model=s['model'],
                                     auto_pause_enabled=s['auto_pause_enabled'], threshold=s['auto_pause_threshold']) for i,s in states.items()],
                      modeltrace=run(['systemctl', 'show', 'modeltrace-ovh', '-p', 'MainPID', '-p', 'MemoryCurrent', '-p', 'ActiveState']))
        (TARGET / 'auto-pause-upgrade-20261019.json').write_text(json.dumps(report, indent=2))
        print(json.dumps(report, ensure_ascii=False), flush=True)
    except Exception:
        if stopped:
            run(['systemctl', 'stop', 'modeltrace-ovh'])
            for name in FILES:
                if existing[name]:
                    shutil.copy2(backup / name, TARGET / name)
                elif (TARGET / name).exists():
                    (TARGET / name).unlink()
            shutil.copy2(backup / 'web-modeltrace.js', JS)
            if (backup / 'state.sqlite3').exists():
                with sqlite3.connect(backup / 'state.sqlite3') as copy, sqlite3.connect(DB) as db:
                    copy.backup(db)
            DROPIN.unlink(missing_ok=True)
            CREDENTIAL.unlink(missing_ok=True)
            run(['systemctl', 'daemon-reload'])
            run(['systemctl', 'start', 'modeltrace-ovh'])
            print('Rolled back independent monitor only', flush=True)
        raise


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Avoid dumping HTTP headers/credentials through traceback diagnostics.
        print('Upgrade did not complete: ' + type(error).__name__, flush=True)
        raise SystemExit(1)
