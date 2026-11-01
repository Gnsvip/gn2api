# Codex lifecycle wire acceptance

Linux-only integration recorder adapted from zuchengchen/sub2api commit
`81fc112416f5d91ee3707e191039bd95973fd825` (release `czc-v2026.10.31.3`).
The checked-in reference uses official Codex CLI 0.166.0, Astra and xhigh.

This runs the official CLI, real tool subprocesses, production gateway service
methods and real HTTP/WebSocket/TLS sockets against synthetic loopback upstreams.
It uses an in-memory account repository. It does not test a deployed server's
authentication middleware, account scheduler, database or real model answers.

Scenarios cover initial WS/prewarm, WS tools, HTTP fallback tools, HTTP 401
reload/refresh, WS 401 reload/refresh, and WS context compaction. Each includes
both models initialization requests. Gateway captures retain per-request sender
ownership, connection IDs, frames, raw data, TLS and credential-write receipts.
The comparator rejects core requests that bypass the gateway. Driver-owned
bootstrap/plugin/analytics calls are retained but excluded from the gateway
projection. Dynamic IDs and clocks are normalized with their relationships;
ordinary JSON map order is not a semantic difference. Typed struct order and
header order remain part of comparison.

Build and compare from `backend/`:

```sh
go build -o codexgolden ./tools/codexgolden
./codexgolden compare -golden REFERENCE_DIRECTORY -candidate GATEWAY_DIRECTORY -v
go test ./tools/codexgolden
```

Recording requires a private user/mount namespace whose system certificate
bundle trusts a local fixture CA. Supply `CODEXGOLDEN_CERT`, `CODEXGOLDEN_KEY`
and `CODEXGOLDEN_CA` pointing to that local fixture. The leaf must cover
localhost, 127.0.0.1, chatgpt.com and auth.openai.com. Do not modify the host's
certificate store. For the default lane, leave `CODEX_CA_CERTIFICATE` and
`SSL_CERT_FILE` unset. Set `CODEXGOLDEN_CUSTOM_CA=1` to run the separate custom-CA
lane: the recorder supplies the fixture CA to both clients and observes actual
HTTP/2 header order, SETTINGS, connection windows, stream IDs and connection
reuse. WebSocket Upgrade remains HTTP/1.1 over rustls. Both modes exercise all
six scenarios; compare each mode against its own official reference.
Install the same official release's `codex-code-mode-host` and
`codex-resources/bwrap` alongside the CLI before testing tool execution.

Inside that namespace, use `-codex`, `-out`, `-models-json`, `-expect-version
0.166.0`, and `-source-commit 6ff1e1ca3b30fc8b914720681fcf5fe62e95f856`.
The default command records the official lane; the `gateway` subcommand
records the gateway lane. Always use a new output directory. `strace` and the
loopback CONNECT allowlist enforce the socket boundary. Use synthetic tokens
only; this recorder must never receive production credentials.

The `lifecycle-custom-ca` golden directory contains the independent official
0.166.0 capture. It shows fresh business HTTP connections for models and each
Responses request, including continuation and 401 replay; auth-client pooling is
tested separately. A record claiming HTTP/2 without real tap evidence is invalid.

The HTTP fallback scenario currently verifies upstream behavior: the client
can reconnect to the already-upgraded local gateway several times before its
own HTTP fallback. Passing the upstream projection is not a claim of identical
downstream handshake status or timing.
