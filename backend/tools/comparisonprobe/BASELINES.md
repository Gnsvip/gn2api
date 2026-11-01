# Codex 0.166.0 comparison coverage

The reference is the official Linux musl CLI 0.166.0, SHA256
`b8a11fedf24b2dfb59a70c9e52b030f9a508dcd49cd2dd0d41956f4c785156b8`.
Captured fixtures are synthetic local-server observations, not live account
acceptance. Live comparisons are separate, private artifacts and are not shipped
with source or release archives.

The harness explicitly enables production's full-synthesis and metadata-order
jitter defaults and records them as `effective_config`. An empty Go Config does
not load Viper defaults. `-canonical-user-agent` fixes the target profile only
inside this isolated harness. Match the account client family/platform as well.

The reference contract at zuchengchen/sub2api `czc-v2026.10.31.3` compares header
order, top-level body key order, embedded typed turn-metadata order, semantic
values, and UUID shape/relationships. Ordinary nested JSON maps may have different
key order. Rebuilding to a configured identity is intentional; compare against
that identity, not an unrelated incoming client profile. These conditions do
not imply that all endpoints or six lifecycle scenarios have passed.

| Endpoint or path | Evidence and checks | Limit |
| --- | --- | --- |
| HTTP `/backend-api/codex/responses` | Official header/body fixtures; `TestGolden_HTTPResponsesHeaderOrder`; Astra/xhigh native typed-input fixture through real `Forward` and passthrough | Different account/session IDs remain isolated; not byte-identical requests |
| WS Responses | Official first turn, tool loop, compaction and 401 fixtures; `TestGolden_WSHandshakeHeaderOrder`, `TestGolden166_WSFrameReplay`; timezone writers/pool tests | Live paired acceptance for this change uses HTTP, not WS |
| GET `/backend-api/codex/models` | `TestGolden166ModelsWireBaseline` and `TestCodexModelsProductionBuilderUsesCapturedTransport` | Eligible OAuth catalog requests only |
| POST `/oauth/token` | HTTP/WS recovery captures; `TestGolden166OAuthRefreshBaseline`, `TestOpenAIRefreshMatches166CapturedJSON`; production JSON/reason tests | Refresh body contract is aligned. Auth identity and transport are not claimed identical. No live credential refresh during acceptance |
| Admin QA/attribution test | `TestComparisonCapturesFinalRequestAndResponse`, mapping/fallback rejection tests, ModelTrace persistence/contract tests | Fixed `codex-http-xhigh-v1` profile is distinct from the native CLI context |
| Request reconstruction | `TestCodex166ReconstructionPreservesClientContract`, native-context fixture with synthesis enabled; `TestCodexStandaloneProbeUsesDefaultWireTransport` | Generic/compatible client instructions and tools remain authoritative; reconstruction is not evidence of model identity |
| Native Tibo and usage snapshot probes | Timezone constructor tests | Separate behavior probes, not model-identity proof. Usage snapshot retains its existing HTTP client |
| Plugins featured/installed/list/suggested, MCP, analytics, accounts/check, user settings | Present in the official six-scenario captures and endpoint inventory | Client background/control requests; no claim that a gateway reproduces the whole client session. Acceptance pins catalog/bootstrap data locally |
| Authorization-code exchange, standalone search, images, realtime calls/sideband, PAT and agent identity | Code-path inventory | No matching official scenario baseline; equivalence unverified |
| Other providers and API-key accounts | Protocol-scope regression | Excluded from the OpenAI OAuth Codex comparison profile |

Native Astra stores base instructions in typed input messages tagged
`model.base_instructions`, and tools in `additional_tools`. Forwarding must not
inject additional top-level default instructions. Untagged compatibility inputs
retain their existing default-instruction behavior.
The legacy input-metadata stripping workaround must also preserve these native
typed requests, including content-item kinds and provenance. Account-scoped
turn IDs may change; semantic message fields must not.

The CLI's custom CA environment settings can select a different TLS backend.
Live acceptance must identify the actual ClientHello, not infer TLS behavior
from a User-Agent. Local bootstrap trust and timezone can be isolated using a
private user/mount namespace without changing host or server configuration.

The existing numeric fingerprint bank is not calibrated for the new fixed
comparison conditions. Preserve its full scores as closed-set candidates;
`bank_applicability=unverified` must not become a confident identity conclusion
or an automatic attribution-based scheduling stop.
