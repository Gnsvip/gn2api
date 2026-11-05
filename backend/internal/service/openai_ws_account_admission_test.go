package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type wsAccountAdmissionRepo struct {
	AccountRepository
	mu      sync.Mutex
	account *Account
}

func (r *wsAccountAdmissionRepo) GetByID(context.Context, int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.account == nil {
		return nil, ErrAccountNotFound
	}
	copy := *r.account
	return &copy, nil
}

func TestWSAccountAdmissionIndependentOfPerformanceGuard(t *testing.T) {
	selected := passthroughLifecycleAccount()
	copy := *selected
	repo := &wsAccountAdmissionRepo{account: &copy}
	svc := &OpenAIGatewayService{accountRepo: repo}
	require.NoError(t, svc.CheckOpenAIWSAccountTurn(context.Background(), selected))
	copy.Schedulable = false
	require.Error(t, svc.CheckOpenAIWSAccountTurn(context.Background(), selected))
	copy.Schedulable = true
	until := time.Now().Add(time.Minute)
	copy.TempUnschedulableUntil = &until
	require.Error(t, svc.CheckOpenAIWSAccountTurn(context.Background(), selected))
	until = time.Now().Add(-time.Second)
	require.NoError(t, svc.CheckOpenAIWSAccountTurn(context.Background(), selected))
	repo.account = nil
	require.Error(t, svc.CheckOpenAIWSAccountTurn(context.Background(), selected))
}

func TestWSAccountManualStopBlocksNextTurnNativeAndHTTPBridge(t *testing.T) {
	for _, bridge := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "http_bridge"}[bridge], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			upstream := newStagedPassthroughConn()
			cfg := passthroughLifecycleConfig()
			svc := newPassthroughLifecycleService(cfg, upstream)
			selected := passthroughLifecycleAccount()
			copy := *selected
			repo := &wsAccountAdmissionRepo{account: &copy}
			svc.accountRepo = repo
			var httpUpstream *httpUpstreamRecorder
			if bridge {
				selected.Extra["openai_apikey_responses_websockets_v2_mode"] = OpenAIWSIngressModeHTTPBridge
				body := "data: " + string(autoContinuationEvent("resp_manual_stop", autoContinuationAnswer)) + "\n\n"
				httpUpstream = &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}}
				svc.httpUpstream = httpUpstream
			}
			server, done := startPassthroughLifecycleServerWithHooks(t, ctx, svc, selected, func(*gin.Context) *OpenAIWSIngressHooks {
				return &OpenAIWSIngressHooks{BeforeTurn: func(turn int) error {
					if turn > 1 {
						return svc.CheckOpenAIWSAccountTurn(ctx, selected)
					}
					return nil
				}}
			})
			defer server.Close()
			client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			require.NoError(t, err)
			defer client.CloseNow()
			frame := []byte(`{"type":"response.create","model":"model","input":"synthetic"}`)
			require.NoError(t, client.Write(ctx, coderws.MessageText, frame))
			if !bridge {
				select {
				case <-upstream.writes:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				upstream.Send(string(autoContinuationEvent("resp_manual_stop", autoContinuationAnswer)))
			}
			for {
				_, payload, e := client.Read(ctx)
				require.NoError(t, e)
				if gjson.GetBytes(payload, "type").String() == "response.completed" {
					break
				}
			}
			repo.mu.Lock()
			repo.account.Schedulable = false
			repo.mu.Unlock()
			require.True(t, selected.Schedulable, "connection retains its stale copy")
			require.NoError(t, client.Write(ctx, coderws.MessageText, frame))
			_, _, err = client.Read(ctx)
			require.Error(t, err)
			select {
			case e := <-done:
				require.Error(t, e)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if bridge {
				require.Len(t, httpUpstream.bodies, 1, "suspended account must not receive a second HTTP request")
			}
			select {
			case payload := <-upstream.writes:
				t.Fatalf("suspended account received next frame: %s", payload)
			default:
			}
		})
	}
}
