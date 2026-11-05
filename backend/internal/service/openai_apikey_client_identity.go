package service

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/tidwall/gjson"
)

// Identify the actual gateway, without claiming a browser or Codex identity.
const openAIAPIKeyGatewayUserAgent = "GN2API"

var openAIDefaultLibraryUserAgent = regexp.MustCompile(`(?i)^(python-urllib|python-requests|python-httpx|go-http-client|curl|wget|node-fetch|axios|okhttp|libcurl|aiohttp)(/[0-9][a-z0-9._+-]*)?$`)

// Called at the final HTTP/WS send boundary. Explicit account settings and
// application identities remain authoritative; only absent/library defaults
// need a descriptive application identity for upstream API compatibility.
func applyOpenAIAPIKeyUserAgent(account *Account, headers http.Header) {
	if account == nil || account.Platform != PlatformOpenAI || account.Type != AccountTypeAPIKey || headers == nil || account.UsesOpenAICodexProtocol() {
		return
	}
	if strings.TrimSpace(account.GetOpenAIUserAgent()) != "" {
		return
	}
	if _, configured := account.HeaderOverrideValue("user-agent"); configured {
		return
	}
	for name, values := range headers {
		if !strings.EqualFold(name, "User-Agent") {
			continue
		}
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value != "" && !openAIDefaultLibraryUserAgent.MatchString(value) {
				return
			}
		}
	}
	for name := range headers {
		if strings.EqualFold(name, "User-Agent") {
			delete(headers, name)
		}
	}
	headers.Set("User-Agent", openAIAPIKeyGatewayUserAgent)
}

const openAIClientSignatureReason = GatewayFailureReason("openai_upstream_client_signature")

// Recognize the observed Cloudflare denial, not arbitrary echoed prompt text
// or a general 403 (which must retain its existing credential/error policy).
func isOpenAIClientSignatureRejection(status int, headers http.Header, body []byte) bool {
	if status != http.StatusForbidden {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(string(body)), "error code: 1010") {
		return true
	}
	if !strings.EqualFold(strings.TrimSpace(headers.Get("Server")), "cloudflare") || !gjson.ValidBytes(body) {
		return false
	}
	for _, path := range []string{"error.code", "code"} {
		if gjson.GetBytes(body, path).String() == "1010" {
			return true
		}
	}
	return false
}
