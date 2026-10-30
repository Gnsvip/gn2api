# Sub2API 0.2.20

## Codex egress transport (experimental)

- Adds an opt-in HTTP and WebSocket wire transport for explicitly allowlisted OpenAI OAuth accounts.
- The transport is disabled by default. Enable `gateway.codex_egress.enabled` and provide specific account IDs in `gateway.codex_egress.account_ids` before it can be selected.
- Verified Codex CLI profiles are limited to `0.163.0` and `0.164.0`, the canonical Ubuntu CLI identity, and `chatgpt.com:443` Codex Responses requests. Other requests continue through the existing transport.
- The production installation reports Codex CLI `0.166.0`; that version is not covered by the captured profiles and will continue using the existing transport.
- HTTP uses the captured TLS profile and zstd request encoding where the request body can be replayed. WebSocket uses the custom Codex TLS handshake and frame transport.

The release is based on the current `Gnsvip/gn2api` main branch and includes its existing changes. The Codex transport itself does not add a database migration or alter billing behavior.
