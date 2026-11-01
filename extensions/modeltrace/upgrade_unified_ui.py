"""Guarded UI-only upgrade. Default: read-only preflight; no database writes or service restarts."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import time

from upgrade_auto_pause import snapshot, run

SOURCE = Path(__file__).resolve().parent
TARGET = Path('/opt/modeltrace-ovh')
JS = Path('/opt/sub2api-custom/modeltrace.js')
NGINX = Path('/etc/nginx/sites-enabled/aifxo.com').resolve()
VERSION = '20261031-tibo-true-1'
FILES = ['modeltrace.js', 'README.md', 'RELEASE_NOTES.md', 'upgrade_unified_ui.py']


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replace_file(source, target):
    with tempfile.NamedTemporaryFile(dir=target.parent, prefix='.modeltrace-ui-', delete=False) as stream:
        temporary = Path(stream.name)
    try:
        shutil.copy2(source, temporary)
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--expected-script-sha256')
    parser.add_argument('--expected-service-sha256')
    args = parser.parse_args()
    if os.geteuid() != 0 or SOURCE == TARGET:
        raise RuntimeError('run as root from a separate extracted release directory')
    manifest = json.loads((SOURCE / 'MANIFEST.json').read_text(encoding='utf-8'))
    for name in FILES + ['service.py', 'upgrade_auto_pause.py']:
        if digest(SOURCE / name) != manifest.get(name):
            raise RuntimeError('release checksum mismatch: ' + name)
    backend_hash = digest(TARGET / 'service.py')
    if backend_hash != manifest['service.py']:
        raise RuntimeError('UI-only upgrade requires the matching Tibo/manual-preview backend')
    script_hash = digest(JS)
    if digest(TARGET / 'modeltrace.js') != script_hash:
        raise RuntimeError('installed web script and extension copy differ; review before upgrade')
    original = NGINX.read_text(encoding='utf-8')
    updated, count = re.subn(r'(/custom/modeltrace\.js\?v=)[A-Za-z0-9_.-]+',
                            lambda match: match[1] + VERSION, original)
    if count != 1:
        raise RuntimeError('expected exactly one existing ModelTrace script injection')
    if re.search(r'^\s*worker_shutdown_timeout\s+(?!0\s*;)', run(['nginx', '-T']), re.M):
        raise RuntimeError('finite nginx worker shutdown timeout requires review')
    if run(['systemctl', 'is-active', 'modeltrace-ovh']) != 'active':
        raise RuntimeError('independent service is not active')
    print(json.dumps(dict(mode='read-only preflight', script_version=VERSION,
                          service_sha256=backend_hash, script_sha256=script_hash,
                          changes=['page script', 'extension documentation', 'nginx script cache version'],
                          database_writes=False, service_restarts=False)), flush=True)
    if not args.apply:
        return
    if args.expected_service_sha256 != backend_hash or args.expected_script_sha256 != script_hash:
        raise RuntimeError('provide unchanged service and script hashes from reviewed preflight')
    before = snapshot()
    monitor_before = run(['systemctl', 'show', 'modeltrace-ovh', '-p', 'MainPID', '-p', 'ExecMainStartTimestamp'])
    backup = TARGET / 'backups' / (time.strftime('%Y%m%d-%H%M%S') + '-unified-ui')
    backup.mkdir(mode=0o700)
    (backup / 'baseline.json').write_text(json.dumps(before, indent=2), encoding='utf-8')
    existed = {name: (TARGET / name).exists() for name in FILES}
    for name in FILES:
        if existed[name]:
            shutil.copy2(TARGET / name, backup / name)
    shutil.copy2(JS, backup / 'web-modeltrace.js')
    shutil.copy2(NGINX, backup / 'nginx.conf')
    changed_nginx = False
    try:
        for name in FILES:
            replace_file(SOURCE / name, TARGET / name)
            (TARGET / name).chmod(0o644)
        replace_file(SOURCE / 'modeltrace.js', JS)
        JS.chmod(0o644)
        if NGINX.read_text(encoding='utf-8') != original:
            raise RuntimeError('nginx changed during upgrade')
        config = backup / 'nginx-updated.conf'
        config.write_text(updated, encoding='utf-8')
        config.chmod(NGINX.stat().st_mode & 0o777)
        replace_file(config, NGINX)
        changed_nginx = True
        run(['nginx', '-t'])
        run(['systemctl', 'reload', 'nginx'])
        after = snapshot()
        if any(before[field] != after[field] for field in ('boot_id', 'units', 'containers')):
            raise RuntimeError('protected service identity changed')
        if any(value != after['hashes'].get(path) for path, value in before['hashes'].items()
               if Path(path).resolve() != NGINX):
            raise RuntimeError('unrelated protected file changed')
        if digest(TARGET / 'service.py') != backend_hash:
            raise RuntimeError('backend changed during upgrade')
        if monitor_before != run(['systemctl', 'show', 'modeltrace-ovh', '-p', 'MainPID', '-p', 'ExecMainStartTimestamp']):
            raise RuntimeError('independent service identity changed')
        if digest(JS) != manifest['modeltrace.js'] or digest(TARGET / 'modeltrace.js') != manifest['modeltrace.js']:
            raise RuntimeError('installed page checksum mismatch')
        report = dict(status='ok', backup=str(backup), script_version=VERSION,
                      script_sha256=digest(JS), service_sha256=backend_hash,
                      all_services_not_restarted=True, database_writes=False,
                      backend_unchanged=True, nginx_graceful_reload=True)
        (TARGET / 'unified-ui-upgrade.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(json.dumps(report), flush=True)
    except Exception:
        for name in FILES:
            if existed[name]:
                replace_file(backup / name, TARGET / name)
            else:
                (TARGET / name).unlink(missing_ok=True)
        replace_file(backup / 'web-modeltrace.js', JS)
        if changed_nginx:
            replace_file(backup / 'nginx.conf', NGINX)
            run(['nginx', '-t'])
            run(['systemctl', 'reload', 'nginx'])
        print('UI files rolled back; no service restart or database write', flush=True)
        raise


if __name__ == '__main__':
    main()
