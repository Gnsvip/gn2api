package openai

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRefreshRejectionClassificationAndRedaction(t *testing.T) {
	for _, tc := range []struct {
		status    int
		body      string
		permanent bool
		code      string
	}{{400, `{"error":"invalid_grant","detail":"secret"}`, true, "invalid_grant"}, {400, `{"error":{"code":"refresh_token_reused","message":"secret"}}`, true, "refresh_token_reused"}, {401, `secret`, true, ""}, {503, `{"error":"secret"}`, false, ""}, {400, `{"error":"invalid_client"}`, false, "invalid_client"}, {429, `secret`, false, ""}} {
		err := NewRefreshTokenError(tc.status, []byte(tc.body))
		require.Equal(t, tc.permanent, err.Permanent)
		require.Equal(t, tc.code, err.Code)
		require.NotContains(t, err.Error(), "secret")
	}
}
