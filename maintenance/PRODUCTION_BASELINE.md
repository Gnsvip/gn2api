# gn2api production baseline

Recorded on 2026-10-30. This branch starts from the source commit reported by
the running binary. It does not claim a reproducible build of that binary.

| Item | Value |
| --- | --- |
| Application | Sub2API 0.2.19 |
| Reported source | `dece26153621959ccd7635814f4b2d08304e681a` |
| Reported build time | `2026-10-28T10:09:03Z` |
| Binary SHA256 | `57b857ab51ecc22657423c1b8144abc96d666491aa10156fef7409b66d71c10f` |
| Binary size | 124469408 bytes |
| Baseline branch | `codex/gn2api-production` |
| Development branch | `codex/codex-egress` |
| Existing main at inventory | `d851a38477597157e78450d30af2d8504764d085` |
| Reference fork | `zuchengchen/sub2api`, tag `czc-v2026.10.30.2` |
| Reference commit | `4ad3bb5d7e2d0ca74b274f05dbd64c3d3c3a3571` |

The existing main is 16 commits ahead of the reported production source. Keep
that history; review its changes separately from the transport work. The live
installation contains a binary and runtime data, not a Git checkout.

## Existing deployment customizations

- `custom-ui/hide-footer-links.js`: existing home-page links, redeem link and
  branding injection. Source copied from the live installation without execution.
- `custom-ui/modeltrace.js`: existing admin ModelTrace UI; reads the logged-in
  administrator's token at runtime, with no embedded credential.
- The separate ModelTrace Python service, its state and configuration remain
  independently managed. This branch does not replace that service.
- Existing logos/favicon and Nginx HTML injection must be preserved during a
  future deployment. The copied scripts are archival source, not automatically
  bundled into the frontend.
- Application listens on loopback port 8080. Nginx also forwards the separate
  `/custom/modeltrace/api` route to loopback port 17861.
- Preserve streaming/WS proxy settings, the 256 MiB request-body limit, branding
  assets, redeem redirects and the private subscription route.
- Actual Nginx/systemd configuration and private customization snapshots stay in
  the operator's private archive, outside this public repository.

## Development and release boundaries

Transport work is opt-in and must preserve billing, account scheduling and
non-OpenAI routes. Do not copy the reference fork's price multipliers, platform
removals, model retirements or full prompt/tool synthesis into this change.
No database schema change is planned.

The experimental `gateway.codex_egress` config has an off-by-default switch
and explicit OpenAI OAuth account ID allowlist. It currently accepts only the
captured 0.163.0/0.164.0 version range, the existing canonical `codex_cli_rs`
Ubuntu identity (no identity header is rewritten), and the Responses path on
`chatgpt.com:443`; every other request stays on the existing Go transport.
The HTTP path uses a fresh HTTP/1.1 connection and zstd-compresses replayable
Responses bodies. The WebSocket path uses a custom TLS ClientHello and raw
upgrade/frame implementation. No HTTP/2 transport lane, model/prompt synthesis,
automatic account selection or production rollout is included. Source fixtures and
profiles were selectively adapted from the LGPL-3.0 `czc-v2026.10.30.2` tag.

Use local mocks and isolated synthetic databases for validation. Do not mirror
production inference requests or reuse active production credentials in tests.
The reference captures cover Codex 0.163.0 and 0.164.0; they do not prove exact
0.166.0 compatibility. A version header alone cannot establish wire fidelity.

Branch pushes run inherited CI/security checks where Actions are enabled.
The inherited release workflow publishes only on `v*` tags or manual dispatch;
neither is part of this preparation. No production deploy workflow is added.

## Before any deployment

Build and verify a candidate with a recorded commit and SHA256. Preserve the
current binary and credentials. Check schema/cache compatibility, background
job ownership, quota/charging consistency and process-local WS state before
running two versions against production data.

The current application shutdown deadline is 5 seconds, even though systemd
allows 5 minutes 30 seconds. A direct restart may interrupt streams. Nginx reload
does not migrate application sessions. Keep the old instance until connections
drain; if coexistence is not validated, use an agreed maintenance window.
Application rollback must not restore an old database over newer transactions.

Production installation, configuration changes, reloads and restarts are not
performed by this branch. Review a concrete rollout and rollback procedure first.
