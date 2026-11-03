package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

type latencyTestDialer struct{ url string }

func (d *latencyTestDialer) Dial(ctx context.Context, _ string, h http.Header, proxy string) (openAIWSClientConn, int, http.Header, error) {
	time.Sleep(30 * time.Millisecond)
	return newDefaultOpenAIWSClientDialer().Dial(ctx, d.url, h, proxy)
}

func TestHTTPToWSLatencySeparatesConnectionAndVisibleContent(t *testing.T) {
	gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: OpenAITTFTModeVisible, expiresAt: time.Now().Add(time.Minute).UnixNano()})
	t.Cleanup(func() { gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{}) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for turn := 0; turn < 2; turn++ {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
			if conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"codex.rate_limits","rate_limits":{}}`)) != nil {
				return
			}
			if conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.output_item.added","item":{"type":"message","content":[]}}`)) != nil {
				return
			}
			time.Sleep(80 * time.Millisecond)
			output := autoContinuationAnswer
			if turn == 0 {
				if conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.output_text.delta","delta":"done"}`)) != nil {
					return
				}
			}
			if turn == 1 {
				output = ""
			} // Completion with no content has no visible TTFT.
			if conn.WriteMessage(websocket.TextMessage, autoContinuationEvent("resp_timing", output)) != nil {
				return
			}
		}
	}))
	defer server.Close()
	s, account, _ := httpBridgeFixture(t, server.URL)
	defer s.openaiWSPool.Close()
	s.openaiWSPool.setClientDialerForTest(&latencyTestDialer{url: "ws" + server.URL[len("http"):]})
	for turn := 0; turn < 2; turn++ {
		c, _ := httpBridgeContext("timing-session")
		// Simulate stale data from an earlier account/attempt in this context.
		c.Set(OpsOpenAIWSFirstVisibleMsKey, int64(99999))
		c.Set(OpsOpenAIWSAttemptKey, int64(99))
		result, err := s.Forward(context.Background(), c, account, autoContinuationBody(true))
		require.NoError(t, err)
		require.True(t, result.OpenAIWSMode)
		require.Equal(t, int64(1), c.GetInt64(OpsOpenAIWSAttemptKey))
		sent := c.GetInt64(OpsOpenAIWSRequestSentMsKey)
		firstEvent := c.GetInt64(OpsOpenAIWSFirstEventMsKey)
		require.GreaterOrEqual(t, firstEvent, sent)
		require.Equal(t, turn > 0, c.GetBool(OpsOpenAIWSConnReusedKey))
		if turn == 0 {
			require.GreaterOrEqual(t, c.GetInt64(OpsOpenAIWSAcquireMsKey), int64(25))
			require.GreaterOrEqual(t, sent, c.GetInt64(OpsOpenAIWSAcquireMsKey))
			require.NotNil(t, result.FirstTokenMs)
			visible := c.GetInt64(OpsOpenAIWSFirstVisibleMsKey)
			require.Equal(t, int64(*result.FirstTokenMs), visible)
			require.GreaterOrEqual(t, visible-firstEvent, int64(60), "empty events must not become content TTFT")
			require.GreaterOrEqual(t, result.Duration.Milliseconds(), visible)
		} else {
			require.Nil(t, result.FirstTokenMs)
			value, _ := c.Get(OpsOpenAIWSFirstVisibleMsKey)
			require.Nil(t, value, "old visible milestone must not survive an empty turn")
		}
	}
}

func TestOpenAIWSLatencyRetryClearsUnreachedStages(t *testing.T) {
	c, _ := httpBridgeContext("failed-retry")
	for _, key := range []string{OpsOpenAIWSRequestSentMsKey, OpsOpenAIWSFirstEventMsKey, OpsOpenAIWSFirstVisibleMsKey, OpsOpenAIWSAcquireMsKey, OpsOpenAIWSWriteMsKey, OpsOpenAIWSQueueWaitMsKey, OpsOpenAIWSConnPickMsKey} {
		c.Set(key, int64(123))
	}
	c.Set(OpsOpenAIWSConnReusedKey, true)
	c.Set(OpsOpenAIWSConnIDKey, "old-connection")
	s := &OpenAIGatewayService{}
	_, err := s.forwardOpenAIWSV2(context.Background(), c, nil, nil, "", "", "", OpenAIWSProtocolDecision{}, false, true, "", "", time.Now().Add(-100*time.Millisecond), 2, "", nil)
	require.Error(t, err)
	require.Equal(t, int64(2), c.GetInt64(OpsOpenAIWSAttemptKey))
	require.GreaterOrEqual(t, c.GetInt64(OpsOpenAIWSAttemptStartMsKey), int64(100))
	for _, key := range []string{OpsOpenAIWSRequestSentMsKey, OpsOpenAIWSFirstEventMsKey, OpsOpenAIWSFirstVisibleMsKey, OpsOpenAIWSAcquireMsKey, OpsOpenAIWSWriteMsKey, OpsOpenAIWSQueueWaitMsKey, OpsOpenAIWSConnPickMsKey, OpsOpenAIWSConnReusedKey, OpsOpenAIWSConnIDKey} {
		value, _ := c.Get(key)
		require.Nil(t, value, key)
	}
}
