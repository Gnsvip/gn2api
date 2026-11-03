# OAuth compatibility request session continuity

OpenCode and other compatible clients can enter through Chat Completions or
Messages while the gateway reconstructs an outbound Codex request. Reconstruction
must retain the conversation boundary across requests and tool follow-ups.

## Conversation evidence

Scheduling and Codex identity construction now use the same ordered header list:

1. `session-id`
2. `session_id`
3. `conversation_id`
4. `x-session-affinity`
5. `x-session-id`
6. `x-opencode-session`
7. `x-conversation-id`

Explicit native client metadata retains its existing precedence. A distinct
`thread-id` still identifies a child thread within the same session. Request IDs,
equal prompt text and `prompt_cache_key` alone do not establish a conversation.
Identity remains scoped by downstream API key and upstream OAuth credential.
An explicit Chat Completions body `prompt_cache_key` is retained independently
of the session header; the typed protocol conversion must not replace it with
the handler's account-affinity hint.

Previously the scheduler accepted these compatible-client headers while the
Codex identity resolver only read `session-id` and `session_id`. This could keep
account routing and cache keys stable while generating a new anonymous outbound
session and synthetic prefix metadata on each request.

## One identity per forwarding attempt

Both compatibility converters resolve fingerprint IDs before synthesizing or
isolating the request, then share that result with the prefix, `client_metadata`
and final header builder. Staged IDs are cleared at entry so account failover or
switching fingerprint mode cannot reuse a previous attempt's state.
For a staged session/full identity, the final compatibility headers are
projected again after legacy overrides so `session_id`/`conversation_id` do not
reappear alongside the canonical `session-id`/`thread-id` and contradict the body.

Retries and tool-result continuations retain their turn ID. New user input starts
a new turn within the existing conversation; this change does not freeze all
turn metadata or combine independent conversations.

## Validation and limits

`openai_compat_session_identity_test.go` exercises the real compatibility
converters with an in-memory upstream, including all seven header aliases,
tool-call/result relationships, model and reasoning preservation, new turns,
child threads, user/account isolation, anonymous cache hints and stale state.

These checks prove request construction and identity continuity. They do not
measure the upstream cache hit ratio or establish the sole cause of a historical
user's low cache percentage. No prompt logging or ongoing traffic capture is
introduced by this fix.
