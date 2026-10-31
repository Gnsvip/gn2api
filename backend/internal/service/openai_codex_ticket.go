package service

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// observeOpenAICodexTicketResponseHeader is a no-op hook kept for wire-profile
// parity with the reference Codex lane. The full x-codex-turn-state ticket
// harvesting feature is not part of gn2api's transport.
func (s *OpenAIGatewayService) observeOpenAICodexTicketResponseHeader(*gin.Context, http.Header) {}

// restoreOpenAICodexTicketIdentity is a no-op for gn2api; the ticket identity
// restore path is not part of gn2api's transport.
func restoreOpenAICodexTicketIdentity(*gin.Context, http.Header) {}
