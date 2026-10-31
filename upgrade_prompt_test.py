"""Guarded independent-extension upgrade. Default: read-only preflight."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import time

from upgrade_auto_pause import snapshot, run, get_json

SOURCE = Path(__file__).resolve().parent
TARGET = Path('/opt/modeltrace-ovh')
DB = Path('/var/lib/modeltrace-ovh/state.sqlite3')
JS = Path('/opt/sub2api-custom/modeltrace.js')
NGINX = Path('/etc/nginx/sites-enabled/aifxo.com').resolve()
VERSION = '20261031-prompt-1'
FILES = ['service.py', 'modeltrace.js', 'test_service.py', 'test_ui.py', 'README.md',
         'activate_server.py', 'upgrade_prompt_test.py', 'RELEASE_NOTES.md']


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def updated_nginx(text):
    updated, count = re.subn(r'(/custom/modeltrace\.js\?v=)[A-Za-z0-9_.-]+',
                            lambda m: m[1] + VERSION, text)
    if count != 1:
        raise RuntimeError('expected one existing ModelTrace script injection')
    def patch_location(match):
        body, count = re.subn(r'client_max_body_size\s+[^;]+;', 'client_max_body_size 16k;', match[2])
        if count != 1:
            raise RuntimeError('expected one ModelTrace API body limit')
        return match[1] + body + match[3]
    updated, count = re.subn(r'(location\s+\^~\s+/custom/modeltrace/api/\s*\{)([^{}]*)(\})',
                            patch_location, updated)
    if count != 1:
        raise RuntimeError('expected one existing ModelTrace API location')
    return updated


def replace_file(source, target):
    temporary = target.with_name(target.name + '.prompt-new')
    shutil.copy2(source, temporary)
    temporary.replace(target)


def running_count(db):
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    return sum(db.execute(f'SELECT count(*) FROM {table} WHERE running=1').fetchone()[0]
               for table in ('monitors', 'prompt_monitors') if table in tables)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='apply after reviewed preflight')
    parser.add_argument('--expected-service-sha256', help='service hash from the reviewed preflight')
    args = parser.parse_args()
    if os.geteuid() != 0 or SOURCE == TARGET:
        raise RuntimeError('run as root from a separate extracted release directory')
    for name in FILES:
        if not (SOURCE / name).is_file():
            raise RuntimeError('release missing: ' + name)
        if name.endswith('.py'):
            compile((SOURCE / name).read_text(encoding='utf-8'), name, 'exec')
    original = NGINX.read_text(encoding='utf-8')
    updated = updated_nginx(original)
    current_hash = digest(TARGET / 'service.py')
    key = Path('/etc/modeltrace-ovh/sub2api-admin-key').read_text().strip()
    capabilities = get_json('http://127.0.0.1:8080/api/v1/admin/accounts/test-capabilities', key)
    if capabilities.get('code') != 0 or (capabilities.get('data') or {}).get('attribution_prompt_v1') is not True:
        raise RuntimeError('install the GN2API prompt-interface fix first')
    if run(['systemctl', 'is-active', 'modeltrace-ovh']) != 'active':
        raise RuntimeError('independent service is not active')
    if re.search(r'^\s*worker_shutdown_timeout\s+(?!0\s*;)', run(['nginx', '-T']), re.M):
        raise RuntimeError('finite nginx worker shutdown timeout requires review')
    report = dict(mode='read-only preflight', service_sha256=current_hash,
                  script_version=VERSION, gateway_compatible=True,
                  changes=['independent extension files', 'additive SQLite tables', 'nginx script version and 16k body limit'])
    print(json.dumps(report, ensure_ascii=False), flush=True)
    if not args.apply:
        return
    if args.expected_service_sha256 != current_hash:
        raise RuntimeError('provide the unchanged service hash from reviewed preflight')
    before = snapshot()
    backup = TARGET / 'backups' / (time.strftime('%Y%m%d-%H%M%S') + '-prompt-test')
    backup.mkdir(mode=0o700)
    (backup / 'baseline.json').write_text(json.dumps(before, indent=2), encoding='utf-8')
    for name in FILES:
        if (TARGET / name).is_file():
            shutil.copy2(TARGET / name, backup / name)
    shutil.copy2(JS, backup / 'web-modeltrace.js')
    shutil.copy2(NGINX, backup / 'nginx.conf')
    stopped = changed_nginx = False
    try:
        deadline = time.monotonic() + 300
        while True:
            with sqlite3.connect(DB, timeout=5) as db:
                db.execute('BEGIN IMMEDIATE')
                if running_count(db) == 0:
                    run(['systemctl', 'stop', 'modeltrace-ovh'])
                    stopped = True
                    break
            if time.monotonic() >= deadline:
                raise RuntimeError('monitor stayed busy; active probes were not interrupted')
            print('Waiting for current independent monitor round', flush=True)
            time.sleep(5)
        with sqlite3.connect(DB) as db, sqlite3.connect(backup / 'state.sqlite3') as dest:
            db.backup(dest)
            old_state = db.execute('SELECT * FROM monitors ORDER BY account_id').fetchall()
            old_history = db.execute('SELECT * FROM results ORDER BY id').fetchall()
        for name in FILES:
            replace_file(SOURCE / name, TARGET / name)
            (TARGET / name).chmod(0o644)
        run(['sudo', '-u', 'modeltrace_ovh', '/opt/modeltrace-ovh/venv/bin/python', '-c',
             "import sys; sys.path.insert(0, '/opt/modeltrace-ovh'); from service import Store; Store('/var/lib/modeltrace-ovh/state.sqlite3')"])
        with sqlite3.connect(DB) as db:
            if db.execute('SELECT * FROM monitors ORDER BY account_id').fetchall() != old_state or \
                    db.execute('SELECT * FROM results ORDER BY id').fetchall() != old_history:
                raise RuntimeError('existing attribution state changed during migration')
        run(['systemctl', 'start', 'modeltrace-ovh'])
        deadline = time.monotonic() + 25
        while True:
            try:
                if get_json('http://127.0.0.1:17861/healthz').get('status') == 'ok':
                    break
            except Exception:
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError('independent service health check failed')
            time.sleep(1)
        if NGINX.read_text(encoding='utf-8') != original:
            raise RuntimeError('nginx changed since preflight')
        replace_file(SOURCE / 'modeltrace.js', JS)
        JS.chmod(0o644)
        temporary = backup / 'nginx-updated.conf'
        temporary.write_text(updated, encoding='utf-8')
        temporary.chmod(NGINX.stat().st_mode & 0o777)
        replace_file(temporary, NGINX)
        changed_nginx = True
        run(['nginx', '-t'])
        run(['systemctl', 'reload', 'nginx'])
        after = snapshot()
        if any(before[field] != after[field] for field in ('boot_id', 'units', 'containers')):
            raise RuntimeError('protected service identity changed')
        if any(value != after['hashes'].get(path) for path, value in before['hashes'].items()
               if Path(path).resolve() != NGINX):
            raise RuntimeError('unrelated protected file changed')
        report = dict(status='ok', backup=str(backup), script_version=VERSION,
                      service_sha256=digest(TARGET / 'service.py'), script_sha256=digest(JS),
                      old_attribution_preserved=True, protected_services_not_restarted=True)
        (TARGET / 'prompt-test-upgrade.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
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
            # Additive tables are harmless to the old service. Preserve later writes.
            run(['systemctl', 'start', 'modeltrace-ovh'])
            print('Independent extension files rolled back; database snapshot retained', flush=True)
        raise


if __name__ == '__main__':
    main()
