"""Publish only the compact UI. No database writes or service restarts."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time

sys.path.insert(0, '/opt/modeltrace-ovh')
from upgrade_auto_pause import snapshot, run

SOURCE = Path(__file__).resolve().parent
TARGET = Path('/opt/modeltrace-ovh')
JS = Path('/opt/sub2api-custom/modeltrace.js')
NGINX = Path('/etc/nginx/sites-enabled/aifxo.com').resolve()
FILES = ['modeltrace.js', 'test_ui.py', 'README.md', 'activate_server.py', 'upgrade_compact_ui.py']
VERSION = '20261020-2'


def main():
    assert os.geteuid() == 0
    assert hashlib.sha256(JS.read_bytes()).hexdigest() == '408f58897cb78fa4fc0aaec117afe79e2b53674798a1dbb5723bd8f82bc64739'
    backend_hash = hashlib.sha256((TARGET / 'service.py').read_bytes()).hexdigest()
    original_config = NGINX.read_text()
    needle = '/custom/modeltrace.js?v=20261020-1'
    assert original_config.count(needle) == 1
    assert not re.search(r'^\s*worker_shutdown_timeout\s+(?!0\s*;)', run(['nginx', '-T']), re.M)
    for name in FILES:
        assert (SOURCE / name).is_file(), 'release file missing'
        if name.endswith('.py'):
            compile((SOURCE / name).read_text(), name, 'exec')
    before = snapshot()
    monitor_before = run(['systemctl', 'show', 'modeltrace-ovh', '-p', 'MainPID', '-p', 'ExecMainStartTimestamp'])
    backup = TARGET / 'backups' / (time.strftime('%Y%m%d-%H%M%S') + '-compact-ui')
    backup.mkdir(mode=0o700)
    (backup / 'baseline.json').write_text(json.dumps(before, indent=2))
    for name in FILES:
        if (TARGET / name).exists():
            shutil.copy2(TARGET / name, backup / name)
    shutil.copy2(JS, backup / 'web-modeltrace.js')
    shutil.copy2(NGINX, backup / 'nginx.conf')
    changed_nginx = False
    try:
        for name in FILES:
            temp = TARGET / (name + '.new')
            shutil.copyfile(SOURCE / name, temp)
            temp.chmod(0o644)
            temp.replace(TARGET / name)
        temp = JS.with_suffix('.js.new')
        shutil.copyfile(SOURCE / 'modeltrace.js', temp)
        temp.chmod(0o644)
        temp.replace(JS)
        assert NGINX.read_text() == original_config, 'nginx changed during release'
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
        assert backend_hash == hashlib.sha256((TARGET / 'service.py').read_bytes()).hexdigest()
        assert monitor_before == run(['systemctl', 'show', 'modeltrace-ovh', '-p', 'MainPID', '-p', 'ExecMainStartTimestamp'])
        report = dict(status='ok', backup=str(backup), all_services_not_restarted=True,
                      backend_unchanged=True, database_writes=False,
                      nginx_graceful_reload=True, script_version=VERSION,
                      script_sha256=hashlib.sha256(JS.read_bytes()).hexdigest())
        (TARGET / 'compact-ui-upgrade-20261020.json').write_text(json.dumps(report, indent=2))
        print(json.dumps(report), flush=True)
    except Exception:
        for name in FILES:
            if (backup / name).exists():
                shutil.copy2(backup / name, TARGET / name)
        shutil.copy2(backup / 'web-modeltrace.js', JS)
        if changed_nginx:
            NGINX.write_text(original_config)
            run(['nginx', '-t'])
            run(['systemctl', 'reload', 'nginx'])
        print('UI files rolled back; no service restart', flush=True)
        raise


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('UI upgrade failed: ' + type(error).__name__, flush=True)
        raise SystemExit(1)
