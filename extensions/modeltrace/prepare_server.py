"""Run from the uploaded release directory as root. Does not reload existing services."""
import hashlib
import json
import os
import pathlib
import pwd
import shutil
import socket
import subprocess
import urllib.request

SOURCE = pathlib.Path(__file__).resolve().parent
TARGET = pathlib.Path('/opt/modeltrace-ovh')


def run(args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True, **kwargs).stdout.strip()


def sql(statement):
    return run(['sudo', '-u', 'postgres', 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-d', 'sub2api', '-At', '-c', statement])


def snapshot():
    files = ['/opt/sub2api/sub2api', '/etc/nginx/nginx.conf', '/etc/nginx/sites-enabled/aifxo.com',
             '/etc/nginx/sites-enabled/api.aifxo.com', '/etc/nginx/sites-enabled/cpa.aifxo.com']
    units = ['sub2api', 'nginx', 'postgresql', 'docker', 'xray-ovh', 'hysteria-ovh']
    return dict(boot_id=pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                units={u: run(['systemctl', 'show', u, '-p', 'MainPID', '-p', 'ExecMainStartTimestamp']) for u in units},
                hashes={p: hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() for p in files if pathlib.Path(p).is_file()},
                containers=run(['docker', 'ps', '--format', '{{.ID}} {{.Names}} {{.Status}}']))


if __name__ == '__main__':
    if os.geteuid() != 0:
        raise SystemExit('root required')
    if TARGET.exists():
        raise SystemExit('refusing to overwrite an existing installation')
    if sql("SELECT count(*) FROM pg_roles WHERE rolname='modeltrace_ovh'") != '0':
        raise SystemExit('role already exists; inspect before proceeding')
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 17861))
    baseline = snapshot()
    TARGET.mkdir(mode=0o755)
    (TARGET / 'deployment-baseline.json').write_text(json.dumps(baseline, indent=2))
    for name in ['service.py', 'qa_policy.py', 'attempts.py', 'evidence.py', 'requirements.txt', 'modeltrace.js', 'modeltrace-ovh.service', 'test_service.py']:
        shutil.copy2(SOURCE / name, TARGET / name)
    shutil.copytree(SOURCE / 'vendor', TARGET / 'vendor')
    run(['useradd', '--system', '--user-group', '--home-dir', '/var/lib/modeltrace-ovh', '--shell', '/usr/sbin/nologin', 'modeltrace_ovh'])
    sql("""CREATE ROLE modeltrace_ovh LOGIN;
        ALTER ROLE modeltrace_ovh SET default_transaction_read_only = on;
        ALTER ROLE modeltrace_ovh SET statement_timeout = '3s';
        GRANT CONNECT ON DATABASE sub2api TO modeltrace_ovh;
        GRANT USAGE ON SCHEMA public TO modeltrace_ovh;
        GRANT SELECT (id,platform,type,status,schedulable,parent_account_id,credentials,extra,deleted_at,
          proxy_id,rate_limit_reset_at,overload_until,temp_unschedulable_until,expires_at,last_used_at)
          ON public.accounts TO modeltrace_ovh;
        GRANT SELECT (id,protocol,host,port,username,password,status,deleted_at,expires_at)
          ON public.proxies TO modeltrace_ovh;""")
    run(['python3', '-m', 'venv', '--without-pip', str(TARGET / 'venv')])
    # Bootstrap pip into this virtualenv only, without apt or changing system Python.
    metadata = json.load(urllib.request.urlopen('https://pypi.org/pypi/pip/json', timeout=30))
    wheel = next(f for f in metadata['urls'] if f['filename'].endswith('.whl'))
    blob = urllib.request.urlopen(wheel['url'], timeout=60).read()
    if hashlib.sha256(blob).hexdigest() != wheel['digests']['sha256']:
        raise SystemExit('pip wheel checksum mismatch')
    wheel_path = SOURCE / wheel['filename']
    wheel_path.write_bytes(blob)
    env = dict(os.environ, PYTHONPATH=str(wheel_path), PIP_DISABLE_PIP_VERSION_CHECK='1')
    py = str(TARGET / 'venv/bin/python')
    print('Installing isolated Python dependencies', flush=True)
    subprocess.run(['nice', '-n', '15', py, '-m', 'pip', 'install', '-r', str(TARGET / 'requirements.txt')], check=True, env=env)
    version = sql("SELECT value FROM settings WHERE key IN ('openai_codex_client_version','openai_codex_client_version_synced') AND value<>'' ORDER BY key LIMIT 1")
    import re
    if not re.fullmatch(r'[0-9]+(?:\.[0-9]+){1,3}(?:-[0-9A-Za-z.]+)?', version):
        raise SystemExit('Cannot determine current Codex client version')
    env_file = pathlib.Path('/etc/modeltrace-ovh.env')
    env_file.write_text('CODEX_CLIENT_VERSION=' + version + '\nMODELTRACE_ORIGINS=https://aifxo.com,https://www.aifxo.com\n')
    env_file.chmod(0o640)
    shutil.chown(env_file, user='root', group='modeltrace_ovh')
    shutil.copy2(TARGET / 'modeltrace-ovh.service', '/etc/systemd/system/modeltrace-ovh.service')
    print(run(['systemd-analyze', 'verify', '/etc/systemd/system/modeltrace-ovh.service']))
    run(['systemctl', 'daemon-reload'])
    run(['systemctl', 'enable', '--now', 'modeltrace-ovh'])
    print('Independent service started; existing application and nginx untouched.', flush=True)
    after = snapshot()
    assert after['boot_id'] == baseline['boot_id']
    assert after['units'] == baseline['units']
    assert after['hashes'] == baseline['hashes']
    print('Existing service PIDs, start times and file hashes unchanged.', flush=True)
