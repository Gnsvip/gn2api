"""Publish the independent extension with a checked, graceful nginx reload."""
import hashlib
import json
import pathlib
import shutil
import subprocess
import urllib.request

ROOT = pathlib.Path('/opt/modeltrace-ovh')
CONFIG = pathlib.Path('/etc/nginx/sites-enabled/aifxo.com').resolve()
BACKUP = ROOT / 'nginx-before-modeltrace.conf'


def run(args):
    return subprocess.run(args, check=True, text=True, capture_output=True)


if __name__ == '__main__':
    health = json.load(urllib.request.urlopen('http://127.0.0.1:17861/healthz', timeout=5))
    assert health['status'] == 'ok'
    assert not BACKUP.exists(), 'Already activated; inspect instead of overwriting backup'
    original = CONFIG.read_text()
    baseline = json.loads((ROOT / 'deployment-baseline.json').read_text())
    assert hashlib.sha256(CONFIG.read_bytes()).hexdigest() == baseline['hashes']['/etc/nginx/sites-enabled/aifxo.com'], 'nginx changed since preparation'
    full = run(['nginx', '-T'])
    assert not any('worker_shutdown_timeout' in x and not x.lstrip().startswith('#') for x in full.stdout.splitlines()), 'Inspect worker shutdown timeout first'
    old_tag = '<script id="aifxo-hide-footer-links" src="/custom/hide-footer-links.js?v=20260722-home-footer"></script></body>'
    assert original.count(old_tag) == 1
    new_tag = old_tag.replace('</body>', '<script defer src="/custom/modeltrace.js?v=20261031-review-2"></script></body>')
    block = '''    # Independent opt-in ModelTrace extension; preserve during nginx redeploys.
    location = /custom/modeltrace.js {
        alias /opt/sub2api-custom/modeltrace.js;
        add_header Cache-Control "no-cache";
    }

    location ^~ /custom/modeltrace/api/ {
        proxy_pass http://127.0.0.1:17861/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header Authorization $http_authorization;
        proxy_set_header X-API-Key $http_x_api_key;
        proxy_set_header Connection "";
        proxy_connect_timeout 2s;
        proxy_read_timeout 15s;
        client_max_body_size 128k;
        access_log off;
    }

'''
    marker = '    location = /custom/hide-footer-links.js {'
    assert original.count(marker) == 1
    updated = original.replace(old_tag, new_tag).replace(marker, block + marker)
    shutil.copy2(CONFIG, BACKUP)
    shutil.copy2(ROOT / 'modeltrace.js', '/opt/sub2api-custom/modeltrace.js')
    master = run(['systemctl', 'show', 'nginx', '-p', 'MainPID']).stdout
    try:
        CONFIG.write_text(updated)
        check = run(['nginx', '-t'])
        print(check.stderr.strip(), flush=True)
        run(['systemctl', 'reload', 'nginx'])
        assert run(['systemctl', 'show', 'nginx', '-p', 'MainPID']).stdout == master
    except Exception:
        shutil.copy2(BACKUP, CONFIG)
        run(['nginx', '-t'])
        run(['systemctl', 'reload', 'nginx'])
        raise
    (ROOT / 'nginx-after-modeltrace.sha256').write_text(hashlib.sha256(CONFIG.read_bytes()).hexdigest())
    print('Published via graceful nginx reload; nginx master PID unchanged.', flush=True)
