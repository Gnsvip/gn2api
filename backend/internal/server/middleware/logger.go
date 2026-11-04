package middleware

import (
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Logger 请求日志中间件
func Logger() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 开始时间
		startTime := time.Now()
		requesttiming.Attach(c, startTime, 0)

		// 请求路径
		path := c.Request.URL.Path

		// 处理请求
		c.Next()
		requesttiming.Finish(c, c.Writer.Status())

		// 跳过健康检查等高频探针路径的日志
		if path == "/health" || path == "/setup/status" {
			return
		}

		endTime := time.Now()
		latency := endTime.Sub(startTime)

		method := c.Request.Method
		statusCode := c.Writer.Status()
		clientIP := ip.GetClientIP(c)
		protocol := c.Request.Proto
		accountID, hasAccountID := c.Request.Context().Value(ctxkey.AccountID).(int64)
		platform, _ := c.Request.Context().Value(ctxkey.Platform).(string)
		model, _ := c.Request.Context().Value(ctxkey.Model).(string)
		reason, rejected := GetIngressRejectReason(c)
		if rejected {
			recordIngressReject(c, reason)
			allowed, droppedSummary := globalIngressRejectAccessSampler.allow(endTime)
			if droppedSummary > 0 {
				logger.FromContext(c.Request.Context()).Info("ingress rejection access logs dropped",
					zap.String("component", "http.access"),
					zap.Uint64("dropped_count", droppedSummary),
					zap.Bool(logger.OpsSystemLogSkipField, true),
				)
			}
			if !allowed {
				return
			}
		}

		fields := []zap.Field{
			zap.String("component", "http.access"),
			zap.Int("status_code", statusCode),
			zap.Int64("latency_ms", latency.Milliseconds()),
			zap.String("client_ip", clientIP),
			zap.String("protocol", protocol),
			zap.String("method", method),
			zap.String("path", path),
		}
		if rejected {
			fields = append(fields,
				zap.String("ingress_reject_reason", string(reason)),
				zap.Bool(logger.OpsSystemLogSkipField, true),
			)
		}
		if hasAccountID && accountID > 0 {
			fields = append(fields, zap.Int64("account_id", accountID))
		}
		if platform != "" {
			fields = append(fields, zap.String("platform", platform))
		}
		if model != "" {
			fields = append(fields, zap.String("model", model))
		}
		for _, key := range []string{"openai_response_id", "openai_upstream_transport", "openai_ws_transport_decision", "openai_ws_transport_reason", "openai_ws_auto_incremental_reason", "openai_ws_auto_incremental_item_type", "openai_ws_auto_incremental_previous_item_type", "openai_ws_auto_incremental_changed_fields",
			"ops_openai_ws_conn_id", "openai_ws_scope_hash", "openai_ws_scope_source", "openai_ws_execution_lane",
			"openai_ws_binding_status", "openai_ws_preferred_conn_id", "openai_ws_preferred_status",
			"openai_ws_acquire_reason", "openai_ws_checkpoint_before", "openai_ws_checkpoint_after",
		} {
			if value := c.GetString(key); value != "" {
				fields = append(fields, zap.String(key, value))
			}
		}
		if value, exists := c.Get("openai_ws_auto_incremental_item_index"); exists {
			if index, ok := value.(int); ok && index >= 0 {
				fields = append(fields, zap.Int("openai_ws_auto_incremental_item_index", index))
			}
		}
		if connected, exists := c.Get("openai_upstream_ws_connected"); exists {
			fields = append(fields, zap.Bool("openai_upstream_ws_connected", connected == true))
		}
		for _, key := range []string{"openai_ws_auto_incremental", "openai_ws_auto_incremental_succeeded", "openai_ws_auto_incremental_replay"} {
			if value, exists := c.Get(key); exists {
				fields = append(fields, zap.Bool(key, value == true))
			}
		}
		// Only bounded numeric milestones of the final WS attempt are added to
		// the existing access record. No request bodies or tool values are logged.
		for _, key := range []string{
			"ops_openai_ws_attempt", "ops_openai_ws_attempt_start_ms",
			"ops_openai_ws_acquire_ms", "ops_openai_ws_queue_wait_ms", "ops_openai_ws_conn_pick_ms",
			"ops_openai_ws_write_ms", "ops_openai_ws_request_sent_ms",
			"ops_openai_ws_first_event_ms", "ops_openai_ws_first_visible_ms",
			"openai_timing_first_event_ms", "openai_timing_first_visible_ms", "openai_timing_first_text_ms",
			"openai_timing_first_content_write_ms", "openai_timing_first_content_flush_return_ms",
		} {
			if value, exists := c.Get(key); exists {
				if ms, ok := value.(int64); ok && ms >= 0 {
					fields = append(fields, zap.Int64(key, ms))
				}
			}
		}
		if value, exists := c.Get("ops_openai_ws_conn_reused"); exists {
			if reused, ok := value.(bool); ok {
				fields = append(fields, zap.Bool("ops_openai_ws_conn_reused", reused))
			}
		}

		l := logger.FromContext(c.Request.Context()).With(fields...)
		l.Info("http request completed", zap.Time("completed_at", endTime))

		if len(c.Errors) > 0 {
			l.Warn("http request contains gin errors", zap.String("errors", c.Errors.String()))
		}
	}
}
