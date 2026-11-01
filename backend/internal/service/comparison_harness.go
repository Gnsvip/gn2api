//go:build comparison_harness

package service

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
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
	h := &ComparisonHarness{
		gateway: &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream, toolCorrector: NewCodexToolCorrector(),
			openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), codexSnapshotThrottle: newAccountWriteThrottle(openAICodexSnapshotPersistMinInterval)},
		tests: &AccountTestService{cfg: cfg, httpUpstream: upstream},
	}
	h.tests.openaiGatewayService = h.gateway
	return h
}

// Bind a fresh exit observation to this runner's transport (which may tunnel
// through a loopback proxy). The caller must verify that tunnel's exit IP.
func (h *ComparisonHarness) SetEgressSnapshot(account *Account, observation *ProxyLatencyInfo) error {
	if !isCodexOAuthAccount(account) || observation == nil || !observation.Success || net.ParseIP(observation.IPAddress) == nil ||
		NormalizeOpenAIRequestTimezone(observation.Timezone) == "" || observation.UpdatedAt.IsZero() ||
		time.Since(observation.UpdatedAt) < -time.Minute || time.Since(observation.UpdatedAt) > 2*time.Hour {
		return errors.New("comparison requires a fresh, verified exit timezone observation")
	}
	info := *observation
	info.EgressFingerprint = codexEgressFingerprint(accountProxyURL(account))
	key := directEgressProxyID
	if account.ProxyID != nil {
		key = *account.ProxyID
	}
	h.gateway.requestTimezoneResolver = NewOpenAIRequestTimezoneResolver(&comparisonTimezoneCache{key: key, info: info}, DefaultOpenAIRequestTimezone)
	return nil
}

type comparisonTimezoneCache struct {
	key  int64
	info ProxyLatencyInfo
}

func (c *comparisonTimezoneCache) GetProxyLatencies(_ context.Context, ids []int64) (map[int64]*ProxyLatencyInfo, error) {
	result := make(map[int64]*ProxyLatencyInfo)
	for _, id := range ids {
		if id == c.key {
			info := c.info
			result[id] = &info
		}
	}
	return result, nil
}

func (*comparisonTimezoneCache) SetProxyLatency(context.Context, int64, *ProxyLatencyInfo) error {
	return errors.New("comparison exit snapshot is read-only")
}

func (h *ComparisonHarness) Forward(c *gin.Context, account *Account, body []byte, audit func(string, any)) error {
	ctx := context.WithValue(c.Request.Context(), comparisonSinkKey{}, comparisonSink(audit))
	c.Request = c.Request.WithContext(ctx)
	_, err := h.gateway.Forward(ctx, c, account, body)
	return err
}

func (h *ComparisonHarness) WebSocket(c *gin.Context, account *Account, audit func(string, any)) error {
	ctx := context.WithValue(c.Request.Context(), comparisonSinkKey{}, comparisonSink(audit))
	c.Request = c.Request.WithContext(ctx)
	conn, err := coderws.Accept(c.Writer, c.Request, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(ResolveOpenAIWSClientReadLimitBytes(h.gateway.cfg))
	_, first, err := ReadOpenAIWSClientMessage(ctx, conn, 30*time.Second, coderws.StatusPolicyViolation, "missing response.create")
	if err != nil {
		return err
	}
	audit("ws_ingress_first", json.RawMessage(first))
	return h.gateway.ProxyResponsesWebSocketFromClient(ctx, c, conn, account, account.GetCredential("access_token"), first, nil)
}

func (h *ComparisonHarness) Admin(c *gin.Context, account *Account, model, prompt string) error {
	c.Set(attributionTestContextKey, true)
	return h.tests.testOpenAIAccountConnection(c, account, model, prompt, "attribution")
}

func ComparisonHeaders(headers http.Header) http.Header { return comparisonHeaders(headers) }
