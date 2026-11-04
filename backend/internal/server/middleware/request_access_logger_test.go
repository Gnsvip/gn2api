package middleware

import (
	"context"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
)

type testLogSink struct {
	mu     sync.Mutex
	events []*logger.LogEvent
}

func (s *testLogSink) WriteLogEvent(event *logger.LogEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *testLogSink) list() []*logger.LogEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*logger.LogEvent, len(s.events))
	copy(out, s.events)
	return out
}

func initMiddlewareTestLogger(t *testing.T) *testLogSink {
	return initMiddlewareTestLoggerWithLevel(t, "debug")
}

func initMiddlewareTestLoggerWithLevel(t *testing.T, level string) *testLogSink {
	t.Helper()
	level = strings.TrimSpace(level)
	if level == "" {
		level = "debug"
	}
	if err := logger.Init(logger.InitOptions{
		Level:       level,
		Format:      "json",
		ServiceName: "sub2api",
		Environment: "test",
		Output: logger.OutputOptions{
			ToStdout: false,
			ToFile:   false,
		},
	}); err != nil {
		t.Fatalf("init logger: %v", err)
	}
	sink := &testLogSink{}
	logger.SetSink(sink)
	t.Cleanup(func() {
		logger.SetSink(nil)
	})
	return sink
}

func TestRequestLogger_GenerateAndPropagateRequestID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(RequestLogger())
	r.GET("/t", func(c *gin.Context) {
		reqID, ok := c.Request.Context().Value(ctxkey.RequestID).(string)
		if !ok || reqID == "" {
			t.Fatalf("request_id missing in context")
		}
		if got := c.Writer.Header().Get(requestIDHeader); got != reqID {
			t.Fatalf("response header request_id mismatch, header=%q ctx=%q", got, reqID)
		}
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/t", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	if w.Header().Get(requestIDHeader) == "" {
		t.Fatalf("X-Request-ID should be set")
	}
}

func TestRequestLogger_KeepIncomingRequestID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(RequestLogger())
	r.GET("/t", func(c *gin.Context) {
		reqID, _ := c.Request.Context().Value(ctxkey.RequestID).(string)
		if reqID != "rid-fixed" {
			t.Fatalf("request_id=%q, want rid-fixed", reqID)
		}
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/t", nil)
	req.Header.Set(requestIDHeader, "rid-fixed")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	if got := w.Header().Get(requestIDHeader); got != "rid-fixed" {
		t.Fatalf("header=%q, want rid-fixed", got)
	}
}

func TestRequestLoggerBoundsIncomingRequestID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(RequestLogger())
	r.GET("/t", func(c *gin.Context) {
		reqID, _ := c.Request.Context().Value(ctxkey.RequestID).(string)
		if len(reqID) != 36 {
			t.Fatalf("request_id length=%d", len(reqID))
		}
		c.Status(http.StatusOK)
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/t", nil)
	req.Header.Set(requestIDHeader, strings.Repeat("r", 1024))
	r.ServeHTTP(w, req)
	if got := len(w.Header().Get(requestIDHeader)); got != 36 {
		t.Fatalf("response request_id length=%d", got)
	}
}

func TestLogger_AccessLogIncludesCoreFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sink := initMiddlewareTestLogger(t)

	r := gin.New()
	r.Use(Logger())
	r.Use(func(c *gin.Context) {
		ctx := c.Request.Context()
		ctx = context.WithValue(ctx, ctxkey.AccountID, int64(101))
		ctx = context.WithValue(ctx, ctxkey.Platform, "openai")
		ctx = context.WithValue(ctx, ctxkey.Model, "gpt-5")
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	r.GET("/api/test", func(c *gin.Context) {
		c.Status(http.StatusCreated)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("status=%d", w.Code)
	}

	events := sink.list()
	if len(events) == 0 {
		t.Fatalf("expected at least one log event")
	}
	found := false
	for _, event := range events {
		if event == nil || event.Message != "http request completed" {
			continue
		}
		found = true
		switch v := event.Fields["status_code"].(type) {
		case int:
			if v != http.StatusCreated {
				t.Fatalf("status_code field mismatch: %v", v)
			}
		case int64:
			if v != int64(http.StatusCreated) {
				t.Fatalf("status_code field mismatch: %v", v)
			}
		default:
			t.Fatalf("status_code type mismatch: %T", v)
		}
		switch v := event.Fields["account_id"].(type) {
		case int64:
			if v != 101 {
				t.Fatalf("account_id field mismatch: %v", v)
			}
		case int:
			if v != 101 {
				t.Fatalf("account_id field mismatch: %v", v)
			}
		default:
			t.Fatalf("account_id type mismatch: %T", v)
		}
		if event.Fields["platform"] != "openai" || event.Fields["model"] != "gpt-5" {
			t.Fatalf("platform/model mismatch: %+v", event.Fields)
		}
	}
	if !found {
		t.Fatalf("access log event not found")
	}
}

func TestLogger_IngressRejectRemainsInStandardAccessLog(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sink := initMiddlewareTestLogger(t)
	r := gin.New()
	r.Use(Logger())
	r.GET("/v1/messages", func(c *gin.Context) {
		MarkIngressRejected(c, IngressRejectInvalidAPIKey)
		c.Status(http.StatusUnauthorized)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/messages", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", w.Code)
	}
	events := sink.list()
	if len(events) != 1 {
		t.Fatalf("events=%d, want 1", len(events))
	}
	if got := events[0].Fields["ingress_reject_reason"]; got != string(IngressRejectInvalidAPIKey) {
		t.Fatalf("ingress_reject_reason=%v", got)
	}
	if got, _ := events[0].Fields[logger.OpsSystemLogSkipField].(bool); !got {
		t.Fatalf("%s must be true", logger.OpsSystemLogSkipField)
	}
}

func TestLogger_AccessLogUsesForwardedClientIPFromTrustedProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sink := initMiddlewareTestLogger(t)

	r := gin.New()
	if err := r.SetTrustedProxies([]string{"104.23.251.120"}); err != nil {
		t.Fatalf("set trusted proxies: %v", err)
	}
	r.Use(Logger())
	r.GET("/api/test", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	req.RemoteAddr = "104.23.251.120:443"
	req.Header.Set("X-Forwarded-For", "203.0.113.42")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}

	for _, event := range sink.list() {
		if event == nil || event.Message != "http request completed" {
			continue
		}
		if got := event.Fields["client_ip"]; got != "203.0.113.42" {
			t.Fatalf("client_ip=%q, want real forwarded ip", got)
		}
		return
	}
	t.Fatalf("access log event not found")
}

func TestLogger_AccessLogSeparatesIncrementalAttemptFromSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sink := initMiddlewareTestLogger(t)
	r := gin.New()
	r.Use(Logger())
	r.GET("/api/test", func(c *gin.Context) {
		c.Set("openai_ws_auto_incremental", true)
		c.Set("openai_ws_auto_incremental_succeeded", false)
		c.Set("openai_ws_auto_incremental_replay", true)
		c.Set("openai_ws_auto_incremental_item_index", 7)
		c.Set("openai_ws_auto_incremental_item_type", "message")
		c.Set("openai_ws_auto_incremental_previous_item_type", "reasoning")
		c.Set("openai_ws_auto_incremental_changed_fields", "arguments")
		c.Status(http.StatusOK) // A streaming 200 alone does not prove success.
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/test", nil))
	for _, event := range sink.list() {
		if event == nil || event.Message != "http request completed" {
			continue
		}
		for key, want := range map[string]bool{
			"openai_ws_auto_incremental":           true,
			"openai_ws_auto_incremental_succeeded": false,
			"openai_ws_auto_incremental_replay":    true,
		} {
			if got := event.Fields[key]; got != want {
				t.Fatalf("%s=%v, want %v", key, got, want)
			}
		}
		if got := event.Fields["openai_ws_auto_incremental_item_index"]; got != int64(7) {
			t.Fatalf("unexpected diagnostic index: %v", got)
		}
		if event.Fields["openai_ws_auto_incremental_item_type"] != "message" || event.Fields["openai_ws_auto_incremental_previous_item_type"] != "reasoning" {
			t.Fatal("missing diagnostic item types")
		}
		if event.Fields["openai_ws_auto_incremental_changed_fields"] != "arguments" {
			t.Fatal("missing field-only mismatch diagnostic")
		}
		return
	}
	t.Fatal("access log event not found")
}

func TestLogger_HealthPathSkipped(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sink := initMiddlewareTestLogger(t)

	r := gin.New()
	r.Use(Logger())
	r.GET("/health", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	if len(sink.list()) != 0 {
		t.Fatalf("health endpoint should not write access log")
	}
}

func TestLogger_WSStageTimingsAreNumericAndOptional(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sink := initMiddlewareTestLogger(t)
	r := gin.New()
	r.Use(Logger())
	r.GET("/timing", func(c *gin.Context) {
		c.Set("ops_openai_ws_attempt", int64(2))
		c.Set("ops_openai_ws_request_sent_ms", int64(45))
		c.Set("ops_openai_ws_first_event_ms", int64(80))
		c.Set("ops_openai_ws_first_visible_ms", int64(2300))
		c.Set("ops_openai_ws_queue_wait_ms", int64(0))
		c.Set("ops_openai_ws_conn_reused", false)
		c.Set("ops_openai_ws_acquire_ms", "must-not-log")
		c.Set("ops_openai_ws_write_ms", nil)
		c.Set("ops_openai_ws_conn_pick_ms", int64(-1))
		c.Set("ops_openai_ws_payload", "private-body")
		c.Status(http.StatusOK)
	})
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/timing", nil))
	for _, event := range sink.list() {
		if event == nil || event.Message != "http request completed" {
			continue
		}
		for key, want := range map[string]int64{"ops_openai_ws_attempt": 2, "ops_openai_ws_request_sent_ms": 45, "ops_openai_ws_first_event_ms": 80, "ops_openai_ws_first_visible_ms": 2300, "ops_openai_ws_queue_wait_ms": 0} {
			if event.Fields[key] != want {
				t.Fatalf("%s=%v, want %d", key, event.Fields[key], want)
			}
		}
		if event.Fields["ops_openai_ws_conn_reused"] != false {
			t.Fatal("missing reuse flag")
		}
		for _, key := range []string{"ops_openai_ws_acquire_ms", "ops_openai_ws_write_ms", "ops_openai_ws_conn_pick_ms", "ops_openai_ws_payload"} {
			if _, exists := event.Fields[key]; exists {
				t.Fatalf("unexpected field %s", key)
			}
		}
		return
	}
	t.Fatal("access log event not found")
}

func TestLogger_AccessLogDroppedWhenLevelWarn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sink := initMiddlewareTestLoggerWithLevel(t, "warn")

	r := gin.New()
	r.Use(RequestLogger())
	r.Use(Logger())
	r.GET("/api/test", func(c *gin.Context) {
		c.Status(http.StatusCreated)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("status=%d", w.Code)
	}

	events := sink.list()
	for _, event := range events {
		if event != nil && event.Message == "http request completed" {
			t.Fatalf("access log should not be indexed when level=warn: %+v", event)
		}
	}
}

func TestLoggerCorrelatesResponsesID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sink := initMiddlewareTestLogger(t)
	router := gin.New()
	router.Use(RequestLogger(), Logger(), ClientRequestID())
	router.POST("/v1/responses", func(c *gin.Context) {
		requesttiming.Context(c, c.Request.Context())
		a := requesttiming.FromGin(c).Begin(123, "websocket")
		a.Event("resp_correlated", "text", "completed", 100, true, true)
		a.Written(true)
		a.Flushed()
		c.Set("ops_openai_ws_conn_id", "oa_ws_123_1")
		c.Set("openai_ws_scope_hash", "0123456789abcdef")
		c.Set("openai_ws_execution_lane", "main")
		c.Set("openai_ws_acquire_reason", "preferred_reused")
		c.Set("openai_ws_checkpoint_before", "ready")
		c.Status(http.StatusOK)
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	request.Header.Set("X-Request-ID", "gateway-correlated")
	router.ServeHTTP(httptest.NewRecorder(), request)
	for _, event := range sink.list() {
		if event.Message != "http request completed" {
			continue
		}
		if event.Fields["openai_response_id"] != "resp_correlated" || event.Fields["request_id"] != "gateway-correlated" {
			t.Fatalf("missing direct correlation: %+v", event.Fields)
		}
		for _, key := range []string{"openai_timing_first_event_ms", "openai_timing_first_visible_ms", "openai_timing_first_text_ms", "openai_timing_first_content_write_ms", "openai_timing_first_content_flush_return_ms", "ops_openai_ws_conn_id", "openai_ws_scope_hash", "openai_ws_execution_lane", "openai_ws_acquire_reason", "openai_ws_checkpoint_before"} {
			if _, ok := event.Fields[key]; !ok {
				t.Fatalf("missing metadata %s", key)
			}
		}
		return
	}
	t.Fatal("missing access record")
}
