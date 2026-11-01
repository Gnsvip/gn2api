# Isolated comparison runner

Build explicitly with `go build -tags comparison_harness ./tools/comparisonprobe`.
The service factory and this command are excluded from ordinary server builds.

The runner reads one `service.Account` JSON object on stdin, with an `Egress`
field containing a fresh `ProxyLatencyInfo` exit observation. It requires an
explicit `-account-id` matching that account and its proxy, drops refresh credentials, and listens
only on a random loopback port. It uses the real Responses `Forward` implementation
and HTTP repository with no database, scheduler, or token-refresh dependencies.
`-audit` must name a new file; existing evidence cannot be overwritten. Requests,
upstream headers, and terminal response events are saved with timestamps. Streaming
deltas are not duplicated. Known tokens and proxy passwords are redacted. The
runner exits after 60 minutes or termination. It uses the production timezone
resolver; when using a loopback tunnel, verify its exit IP against the observation
before starting. The optional `-websocket` listener uses the production WS
forwarder and retains ingress messages, final outgoing messages, handshake
status, and terminal response events. It does not force the CLI to use WS;
verify a successful Upgrade and absence of HTTP fallback in each result.

This is an opt-in acceptance tool, not a production server. The comparison build
fixes Codex HTTP transport and does not reproduce every production setting,
plugin, scheduler, accounting, or credential-refresh path. A passing sample is not proof
of upstream model identity. Live tests consume the selected account's quota.

The synthetic CLI 0.166.0 Astra/xhigh request fixture lives under
`internal/pkg/codexwire/testdata/golden/0.166.0/astra_xhigh_controlled_http.json`.
It tests typed instruction/tool/context preservation through the real forwarding
pipeline. It is explicitly not evidence from a live account.
