"""Loopback-only acceptance: compiled Go fixture + Nginx + official Codex.

python3 deploy/test_balance_denial_ingress.py --binary /path/middleware.test \
    --codex /path/codex --output /path/result.json
No production credentials, upstream calls, or running server configuration.
"""
import argparse
import asyncio
import collections
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


class Client:
    def __init__(self, process):
        self.process = process
        self.sequence = 0
        self.pending = {}
        self.errors = []
        self.turns = asyncio.Queue()
        self.reader = asyncio.create_task(self.read())

    async def send(self, obj):
        self.process.stdin.write((json.dumps(obj) + "\n").encode())
        await self.process.stdin.drain()

    async def rpc(self, method, params):
        self.sequence += 1
        future = asyncio.get_running_loop().create_future()
        self.pending[self.sequence] = future
        await self.send({"id": self.sequence, "method": method, "params": params})
        return await asyncio.wait_for(future, 30)

    async def read(self):
        async for line in self.process.stdout:
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if "id" in obj and "method" not in obj:
                future = self.pending.pop(obj["id"], None)
                if future and not future.done():
                    if "error" in obj:
                        future.set_exception(RuntimeError(str(obj["error"])))
                    else:
                        future.set_result(obj.get("result"))
                continue
            if "id" in obj:
                await self.send({"id": obj["id"], "error": {"code": -32601, "message": "Disabled in fixture"}})
            params = obj.get("params", {})
            if obj.get("method") == "error":
                self.errors.append({"willRetry": params.get("willRetry"), "error": params.get("error")})
            elif obj.get("method") == "turn/completed":
                await self.turns.put(params["turn"])


def fetch(url, key=None, data=None, extra_headers=None):
    headers = {"Authorization": "Bearer " + key} if key else {}
    headers.update(extra_headers or {})
    request = urllib.request.Request(url, data=data, headers=headers)
    try:
        response = urllib.request.urlopen(request, timeout=5)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, response.read()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def early_denial(port, ua):
    with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
        started = time.monotonic()
        sock.sendall(("POST /v1/responses HTTP/1.1\r\nHost: localhost\r\n"
                      "Authorization: Bearer fixture-99\r\nAccept: text/event-stream\r\n"
                      f"User-Agent: {ua}\r\nContent-Length: 16000000\r\n\r\n").encode())
        # Deliberately send no history bytes. Must receive a terminal response.
        data = b""
        while b"insufficient_quota" not in data:
            part = sock.recv(4096)
            assert part, "closed without quota response"
            data += part
        return {"status": int(data.split(b" ")[1]), "body_bytes_sent": 0,
                "elapsed_ms": round((time.monotonic() - started) * 1000, 2)}


async def native_case(args, root, base, user, ws, large):
    home, work = root / f"home-{user}", root / f"work-{user}"
    home.mkdir()
    work.mkdir()
    (home / "config.toml").write_text(f'''model = "gpt-6-astra"
model_provider = "fixture"
approval_policy = "never"
sandbox_mode = "read-only"
project_doc_max_bytes = 0
[features]
apps = false
plugins = false
multi_agent = false
memories = false
[analytics]
enabled = false
[history]
persistence = "none"
[model_providers.fixture]
name = "Loopback balance rejection fixture"
base_url = "{base}/v1"
wire_api = "responses"
env_key = "GN2API_FIXTURE_KEY"
requires_openai_auth = false
supports_websockets = {str(ws).lower()}
''')
    process = await asyncio.create_subprocess_exec(str(args.codex), "app-server", "--stdio",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        cwd=work, limit=4*1024*1024, env=dict(os.environ, CODEX_HOME=str(home), GN2API_FIXTURE_KEY=f"fixture-{user}"))
    client = Client(process)
    report = {"user": user, "ws": ws, "large": large, "turns": []}
    def request_count():
        _, body = fetch(base + "/fixture/stats")
        return sum(n for key, n in json.loads(body)["counts"].items() if key.endswith(f" fixture-{user}"))
    try:
        await client.rpc("initialize", {"clientInfo": {"name": "codex_vscode", "version": "1.137.0"}, "capabilities": {"experimentalApi": True}})
        await client.send({"method": "initialized", "params": {}})
        thread = await client.rpc("thread/start", {"cwd": str(work), "ephemeral": True, "model": "gpt-6-astra", "modelProvider": "fixture", "approvalPolicy": "never", "sandbox": "read-only"})
        report["requests_before_turns"] = await asyncio.to_thread(request_count)
        for number in (1, 2):
            before = len(client.errors)
            prompt = "Synthetic quota rejection test. " + ("é" * 750000 if large else "Reply OK.")
            await client.rpc("turn/start", {"threadId": thread["thread"]["id"], "input": [{"type": "text", "text": prompt}]})
            turn = await asyncio.wait_for(client.turns.get(), 35)
            errors = client.errors[before:]
            report["turns"].append({"status": turn.get("status"), "errors": errors, "gateway_requests_so_far": await asyncio.to_thread(request_count)})
            assert turn.get("status") == "failed", report
            assert len(errors) == 1 and errors[0].get("willRetry") is False, report
            assert errors[0].get("error", {}).get("codexErrorInfo") == "usageLimitExceeded", report
        report["requests_before_idle"] = await asyncio.to_thread(request_count)
        for elapsed in range(20, 141, 20):
            await asyncio.sleep(20)
            assert process.returncode is None and len(client.errors) == 2, report
            assert await asyncio.to_thread(request_count) == report["requests_before_idle"], report
            print(json.dumps({"phase": "native_idle", "user": user, "seconds": elapsed}), flush=True)
        report.update(passed=True, idle_seconds=140)
    except Exception as error:
        report.update(passed=False, error_type=type(error).__name__, errors=client.errors)
    finally:
        if process.returncode is None:
            process.terminate()
        await process.wait()
        await client.reader
    print(json.dumps({"phase": "native_case_complete", **report}), flush=True)
    return report


async def run(args):
    report = {}
    with tempfile.TemporaryDirectory(prefix="gn2api-balance-") as folder:
        root = Path(folder)
        ready = root / "ready"
        with (root / "fixture.log").open("w") as log:
            fixture = subprocess.Popen([str(args.binary), "-test.run=^TestBalanceDenialNativeHarness$", "-test.timeout=8m"],
                stdout=log, stderr=subprocess.STDOUT, env=dict(os.environ, GN2API_BALANCE_FIXTURE=str(ready)))
            nginx = None
            try:
                for _ in range(100):
                    if ready.exists(): break
                    assert fixture.poll() is None, (root / "fixture.log").read_text()
                    await asyncio.sleep(.1)
                direct = ready.read_text().strip()
                port = free_port()
                base = f"http://127.0.0.1:{port}"
                deploy = Path(__file__).resolve().parent
                config = root / "nginx.conf"
                config.write_text(f'''pid {root}/nginx.pid;
error_log {root}/nginx-error.log warn;
events {{ worker_connections 1024; }}
http {{
    access_log off;
    include {deploy}/nginx/gn2api-ingress-http.conf;
    server {{
        listen 127.0.0.1:{port};
        client_max_body_size 256m;
        include {deploy}/nginx/gn2api-ingress-server.conf;
        location / {{
            proxy_pass {direct};
            proxy_buffering off;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
        }}
    }}
}}
''')
                subprocess.run(["nginx", "-t", "-p", str(root), "-c", str(config)], check=True, capture_output=True)
                nginx = subprocess.Popen(["nginx", "-p", str(root), "-c", str(config), "-g", "daemon off;"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                for _ in range(100):
                    try:
                        fetch(base + "/fixture/stats")
                        break
                    except OSError:
                        await asyncio.sleep(.05)
                report["early_denial"] = [early_denial(port, ua) for ua in ("OpenAI/Python", "codex_exec/0.166.0")]
                # A bounded local burst must be rejected at Nginx, before Go.
                codes = collections.Counter()
                for _ in range(240):
                    status, body = fetch(base + "/v1/responses", "fixture-98", b"{}")
                    codes[json.loads(body)["error"]["code"]] += 1
                report["edge_burst"] = dict(codes)
                assert codes["ingress_rate_limited"] > 0
                report["native"] = await asyncio.gather(*(native_case(args, root, base, user, ws, large) for user, ws, large in [
                    (11, False, False), (12, False, True), (21, True, False), (22, True, True)]
                    if ("ws" if ws else "http") in args.transports))
                _, stats = fetch(direct + "/fixture/stats")
                report["stats_before_funding"] = json.loads(stats)
                assert report["stats_before_funding"]["passed"] == 0
                for case in report["native"]:
                    counts = report["stats_before_funding"]["counts"]
                    case["gateway_requests"] = sum(n for key, n in counts.items() if key.endswith(f" fixture-{case['user']}"))
                    # Native WS may establish a prewarm connection in addition
                    # to turns. Compare the observed count, not an assumed one.
                    assert case["passed"] and case["gateway_requests"] == case["requests_before_idle"], case
                fetch(direct + "/fixture/fund", data=b"")
                status, body = fetch(base + "/v1/responses", "fixture-11", b'{"input":"funded"}')
                assert status == 200 and json.loads(body)["funded"] is True
                report["recharge_passed"] = True
                status, body = fetch(base + "/v1/responses", "fixture-11", b"{}", {"X-Fixture-Upstream-Status": "429"})
                assert status == 429 and json.loads(body)["error"]["code"] == "upstream_rate_limit"
                report["funded_upstream_429_unchanged"] = True
                report["passed"] = True
            finally:
                if nginx is not None:
                    nginx.terminate()
                    nginx.wait(timeout=10)
                (root / "ready.stop").touch()
                try:
                    fixture.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    fixture.terminate()
                    fixture.wait(timeout=5)
                args.output.write_text(json.dumps(report, indent=2) + "\n")
                print(json.dumps(report), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--codex", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--transports", nargs="*", choices=["http", "ws"], default=["http", "ws"])
    asyncio.run(run(parser.parse_args()))
