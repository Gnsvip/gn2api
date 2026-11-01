package openai

import "context"

type refreshReasonKey struct{}

// Refresh reasons are diagnostics, not an authentication or retry policy.
// Values match Codex CLI 0.166.0 login/src/oauth/client.rs.
func WithRefreshReason(ctx context.Context, reason string) context.Context {
	switch reason {
	case "proactive_expiry", "scheduled", "unauthorized_recovery", "forced":
		return context.WithValue(ctx, refreshReasonKey{}, reason)
	default:
		return ctx
	}
}

func RefreshReason(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	reason, _ := ctx.Value(refreshReasonKey{}).(string)
	return reason
}
