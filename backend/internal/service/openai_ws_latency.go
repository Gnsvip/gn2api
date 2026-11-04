package service

import "github.com/gin-gonic/gin"

const (
	OpsOpenAIWSAttemptKey = "ops_openai_ws_attempt"
	// Milestones are offsets from Forward's start, including earlier retries.
	OpsOpenAIWSAttemptStartMsKey = "ops_openai_ws_attempt_start_ms"
	OpsOpenAIWSRequestSentMsKey  = "ops_openai_ws_request_sent_ms"
	OpsOpenAIWSFirstEventMsKey   = "ops_openai_ws_first_event_ms"
	OpsOpenAIWSFirstVisibleMsKey = "ops_openai_ws_first_visible_ms"
	// Durations refer only to the latest WS attempt. Queue/pick durations are
	// components of acquisition, not additional time to sum with it.
	OpsOpenAIWSAcquireMsKey = "ops_openai_ws_acquire_ms"
	OpsOpenAIWSWriteMsKey   = "ops_openai_ws_write_ms"
)

// Never attribute an earlier attempt's content or connection to a retry or
// another account. Nil values are omitted by the access logger.
func clearOpenAIWSLatency(c *gin.Context) {
	if c == nil {
		return
	}
	for _, key := range openAIWSDiagnosticKeys {
		c.Set(key, nil)
	}
	for _, key := range []string{
		OpsOpenAIWSAttemptKey, OpsOpenAIWSAttemptStartMsKey,
		OpsOpenAIWSRequestSentMsKey, OpsOpenAIWSFirstEventMsKey,
		OpsOpenAIWSFirstVisibleMsKey, OpsOpenAIWSAcquireMsKey, OpsOpenAIWSWriteMsKey,
		OpsOpenAIWSQueueWaitMsKey, OpsOpenAIWSConnPickMsKey,
		OpsOpenAIWSConnReusedKey, OpsOpenAIWSConnIDKey,
	} {
		c.Set(key, nil)
	}
}
