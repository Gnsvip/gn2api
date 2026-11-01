package openai

import "context"

type codexClientUAContextKey struct{}

// WithCodexClientUserAgent carries the account-level device-profile UA through
// background calls (OAuth refresh, token exchange, whoami) so they use the same
// family/platform identity as inference.
func WithCodexClientUserAgent(ctx context.Context, userAgent string) context.Context {
	if ctx == nil || userAgent == "" {
		return ctx
	}
	return context.WithValue(ctx, codexClientUAContextKey{}, userAgent)
}

// CodexClientUserAgentFromContext returns the account-level override, if any.
func CodexClientUserAgentFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx.Value(codexClientUAContextKey{}).(string)
	return value
}

type codexAuthAccountContextKey struct{}

// WithCodexAuthAccount isolates the transport owned by a persisted account's
// auth state. The local account ID is never sent as an upstream header.
func WithCodexAuthAccount(ctx context.Context, accountID int64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, codexAuthAccountContextKey{}, accountID)
}

func CodexAuthAccountFromContext(ctx context.Context) int64 {
	if ctx != nil {
		id, _ := ctx.Value(codexAuthAccountContextKey{}).(int64)
		return id
	}
	return 0
}
