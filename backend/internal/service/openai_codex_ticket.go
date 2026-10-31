package service

import (
	"maps"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// observeOpenAICodexTicketResponseHeader is a no-op hook kept for wire-profile
// parity with the reference Codex lane. The full x-codex-turn-state ticket
// harvesting feature is not part of gn2api's transport.
func (s *OpenAIGatewayService) observeOpenAICodexTicketResponseHeader(*gin.Context, http.Header) {}

// restoreOpenAICodexTicketIdentity is a no-op for gn2api; the ticket identity
// restore path is not part of gn2api's transport.
func restoreOpenAICodexTicketIdentity(*gin.Context, http.Header) {}

// Ticket / Tibo probe shared constants. The full ticket-harvesting lifecycle is
// not part of gn2api, but these names are referenced by the Tibo probe and by
// extra-key sanitization.
const (
	openAICodexTicketExtraKeyPrefix        = "codex_turn_ticket:"
	openAICodexTicketRevokedExtraKeyPrefix = "codex_turn_ticket_revoked:"
	openAICookieWSExtraKeyPrefix           = "codex_cookie_ws:"
	openAICodexTicketDefaultModel          = "gpt-6-astra"
	openAICodexTicketDefaultSolModel       = "gpt-5.6-sol"
)

// isOpenAITiboAccount is any OpenAI credential that can answer the Astra Tibo
// probe: ChatGPT OAuth / setup-token, and API-key (including compatible
// gateways with a custom base_url).
func isOpenAITiboAccount(account *Account) bool {
	if account == nil || !account.IsOpenAI() {
		return false
	}
	return account.IsOpenAIOAuthLike() || account.Type == AccountTypeAPIKey
}

// openAICodexSuccessfulCompletionModel extracts the model from a successful
// response.completed SSE event.
func openAICodexSuccessfulCompletionModel(payload []byte, eventType string) (string, bool) {
	if len(payload) == 0 || !gjson.ValidBytes(payload) {
		return "", false
	}
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		eventType = strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	}
	if eventType == "response.completed" {
		if !openAICodexTicketJSONValueEmpty(payload, "error") || !openAICodexTicketJSONValueEmpty(payload, "response.error") {
			return "", false
		}
		status := strings.TrimSpace(gjson.GetBytes(payload, "response.status").String())
		if status != "" && status != "completed" {
			return "", false
		}
		model := strings.TrimSpace(gjson.GetBytes(payload, "response.model").String())
		if model == "" {
			return "", false
		}
		return model, true
	}
	return "", false
}

func openAICodexTicketJSONValueEmpty(payload []byte, path string) bool {
	value := gjson.GetBytes(payload, path)
	return !value.Exists() || value.Type == gjson.Null
}

func IsOpenAICodexTicketExtraKey(key string) bool {
	return strings.HasPrefix(key, openAICodexTicketExtraKeyPrefix) || strings.HasPrefix(key, openAICodexTicketRevokedExtraKeyPrefix) ||
		strings.HasPrefix(key, openAICookieWSExtraKeyPrefix) || strings.HasPrefix(key, openAITiboVerdictExtraKeyPrefix)
}

func MergeOpenAICodexTicketExtra(extra, current map[string]any) map[string]any {
	result := maps.Clone(extra)
	for key := range result {
		if IsOpenAICodexTicketExtraKey(key) {
			delete(result, key)
		}
	}
	for key, value := range current {
		if strings.HasPrefix(key, openAITiboVerdictExtraKeyPrefix) {
			if result == nil {
				result = make(map[string]any)
			}
			result[key] = value
		}
	}
	return result
}
