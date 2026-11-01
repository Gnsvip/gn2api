package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/stretchr/testify/require"
)

func TestOpenAIRefreshMatches166CapturedJSON(t *testing.T) {
	raw, err := os.ReadFile("../pkg/codexwire/testdata/golden/0.166.0/http_unauthorized_refresh.jsonl")
	require.NoError(t, err)
	var expected map[string]string
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		var rec struct {
			Target string            `json:"target"`
			Body   map[string]string `json:"body_json"`
		}
		// Other records may carry non-string Responses fields.
		var envelope map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(line, &envelope))
		if string(envelope["target"]) != `"/oauth/token"` {
			continue
		}
		require.NoError(t, json.Unmarshal(line, &rec))
		expected = rec.Body
	}
	require.NotNil(t, expected)
	type received struct {
		ContentType string
		Body        map[string]string
	}
	seen := make(chan received, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		seen <- received{r.Header.Get("Content-Type"), body}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"fake-access","refresh_token":"fake-refresh"}`))
	}))
	defer server.Close()
	svc := &openaiOAuthService{tokenURL: server.URL}
	_, err = svc.RefreshToken(openai.WithRefreshReason(context.Background(), "unauthorized_recovery"), "rt-golden", "")
	require.NoError(t, err)
	got := <-seen
	require.Equal(t, "application/json", got.ContentType)
	require.Equal(t, expected, got.Body)
}
