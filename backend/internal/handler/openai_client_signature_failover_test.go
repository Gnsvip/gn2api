//go:build unit

package handler

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestOpenAIClientSignatureHandlerFailover(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/v1/chat/completions"} {
		for _, outcome := range []string{"next_account_succeeds", "all_rejected"} {
			t.Run(path+"/"+outcome, func(t *testing.T) {
				repo, upstream, _, router := newOpenAICredentialHandler(t, service.AccountTypeAPIKey, func(cfg *config.Config, accounts []service.Account) {
					cfg.Gateway.OpenAIWS.Enabled = false
					for i := range accounts {
						accounts[i].Type = service.AccountTypeAPIKey
						accounts[i].Credentials = map[string]any{"api_key": "test-key", "pool_mode": true, "pool_mode_retry_count": 10}
						accounts[i].Extra = map[string]any{"openai_responses_supported": true}
					}
				})
				upstream.accountStatuses = map[int64]int{801: http.StatusForbidden}
				upstream.accountResponses = map[int64]string{801: "error code: 1010"}
				if outcome == "all_rejected" {
					upstream.accountStatuses[802] = http.StatusForbidden
					upstream.accountResponses[802] = "error code: 1010"
				}
				body := `{"model":"gpt-6-astra","input":"hello","stream":true}`
				if strings.Contains(path, "chat") {
					body = `{"model":"gpt-6-astra","messages":[{"role":"user","content":"hello"}],"stream":true}`
				}
				req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
				req.Header.Set("User-Agent", "Python-urllib/3.12")
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				require.Equal(t, []int64{801, 802}, upstream.hits, "1010 must never consume the ten same-account retries")
				require.Equal(t, []string{"GN2API", "GN2API"}, upstream.userAgents)
				require.Empty(t, repo.errorIDs())
				require.Empty(t, repo.setTempIDs)
				if outcome == "all_rejected" {
					require.Equal(t, http.StatusBadGateway, w.Code)
				} else {
					require.Equal(t, http.StatusOK, w.Code)
					if strings.Contains(path, "chat") {
						require.Contains(t, w.Body.String(), "[DONE]")
					} else {
						require.Contains(t, w.Body.String(), "response.completed")
					}
				}
			})
		}
	}
}
