package requesttiming

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestRequestTimingGinTurnsCorrelateAndArchiveOnce(t *testing.T) {
	dir := t.TempDir()
	stop, err := Start(filepath.Join(dir, "app.log"))
	require.NoError(t, err)
	defer stop()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/v1/responses", nil)
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), ctxkey.RequestID, "gateway-id"))
	Attach(c, time.Now().Add(-11*time.Second), 0)
	// This middleware can run later than the access logger's Attach.
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), ctxkey.ClientRequestID, "client-id"))
	BeginTurn(c, 1)
	ctx := Context(c, c.Request.Context())
	a := FromContext(ctx).Begin(10, "websocket")
	a.Event("resp_slow", "tool_input", "completed", 99, true, false)
	Finish(c, 101)
	Finish(c, 101)
	BeginTurn(c, 2)
	a = FromGin(c).Begin(10, "websocket")
	a.Event("resp_fast", "text", "completed", 99, true, true)
	Finish(c, 101)
	require.Equal(t, "resp_fast", c.GetString("openai_response_id"))
	files, err := filepath.Glob(filepath.Join(dir, "request-timing", "timing-*.jsonl"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	b, err := os.ReadFile(files[0])
	require.NoError(t, err)
	var s Summary
	require.NoError(t, json.Unmarshal(b, &s), "only one eligible summary, not duplicated")
	require.Equal(t, "gateway-id", s.RequestID)
	require.Equal(t, "client-id", s.ClientRequestID)
	require.Equal(t, 1, s.Turn)
	require.Equal(t, "resp_slow", s.Attempts[0].ResponseID)
}
