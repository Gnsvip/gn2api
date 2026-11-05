package service

import (
	"strings"

	"github.com/tidwall/gjson"
)

// A delivered terminal failure is observable for account health, but must NOT
// unwrap to UpstreamFailoverError: the handler must never replay delivered output.
type openAIStreamTerminalError struct {
	statusCode     int
	message        string
	healthEligible bool
}

func (e *openAIStreamTerminalError) Error() string {
	return "upstream response failed: " + e.message
}

func newOpenAIStreamTerminalError(payload []byte, message string) *openAIStreamTerminalError {
	status := openAIStreamFailedEventSemanticStatus(payload, message)
	return &openAIStreamTerminalError{
		statusCode: status,
		message:    truncateString(sanitizeUpstreamErrorMessage(strings.TrimSpace(message)), 1024),
		// Authentication, policy, malformed input and provider-wide capacity
		// already have separate handling. Never punish an account for these.
		healthEligible: status >= 500 && status < 600 &&
			openAIStreamHasServerFailureSignal(payload) &&
			openAIStreamFailedEventShouldFailover(payload, message) &&
			!isOpenAIRequestScopedCapacityShed(message, payload),
	}
}

// The legacy display status defaults to 502 for unknown stream errors. That
// fallback alone is not evidence that the selected credential is unhealthy.
func openAIStreamHasServerFailureSignal(payload []byte) bool {
	for _, path := range openAIStreamErrorStatusPaths {
		if status := gjson.GetBytes(payload, path).Int(); status >= 500 && status < 600 {
			return true
		}
	}
	for _, path := range []string{"response.error.code", "error.code", "code", "response.error.type", "error.type"} {
		switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, path).String())) {
		case "upstream_error", "server_error", "internal_error", "internal_server_error", "service_unavailable":
			return true
		}
	}
	return false
}
