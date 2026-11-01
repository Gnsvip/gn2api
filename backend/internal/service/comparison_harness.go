//go:build comparison_harness

package service

import (
	"context"
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
)

// ComparisonHarness is built only into the explicitly selected acceptance tool.
// It uses the production Forward implementation with no database or scheduling
// dependencies, so it cannot update the production account or refresh tokens.
type ComparisonHarness struct {
	gateway *OpenAIGatewayService
	tests   *AccountTestService
}

func NewComparisonHarness(cfg *config.Config, upstream HTTPUpstream) *ComparisonHarness {
	return &ComparisonHarness{
		gateway: &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream, toolCorrector: NewCodexToolCorrector(),
			openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), codexSnapshotThrottle: newAccountWriteThrottle(openAICodexSnapshotPersistMinInterval)},
		tests: &AccountTestService{cfg: cfg, httpUpstream: upstream},
	}
}

func (h *ComparisonHarness) Forward(c *gin.Context, account *Account, body []byte, audit func(string, any)) error {
	ctx := context.WithValue(c.Request.Context(), comparisonSinkKey{}, comparisonSink(audit))
	c.Request = c.Request.WithContext(ctx)
	_, err := h.gateway.Forward(ctx, c, account, body)
	return err
}

func (h *ComparisonHarness) Admin(c *gin.Context, account *Account, model, prompt string) error {
	c.Set(attributionTestContextKey, true)
	return h.tests.testOpenAIAccountConnection(c, account, model, prompt, "attribution")
}

func ComparisonHeaders(headers http.Header) http.Header { return comparisonHeaders(headers) }
