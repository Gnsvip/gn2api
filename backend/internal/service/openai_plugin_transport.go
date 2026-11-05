package service

import (
	"fmt"
	"net/http"
	"net/http/httptrace"

	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
)

func (s *OpenAIGatewayService) SetPluginManager(manager *PluginManager) {
	s.pluginManager = manager
}

// doOpenAIUpstream 只在 OpenAI OAuth 能力绑定已启用时把真实请求交给插件。
// 插件返回标准 http.Response，响应解析、错误映射、SSE 和计费仍由现有核心链处理。
func (s *OpenAIGatewayService) doOpenAIUpstream(request *http.Request, proxyURL string, account *Account) (*http.Response, error) {
	return s.doOpenAIUpstreamWithAuthRecovery(request, proxyURL, account)
}

func (s *OpenAIGatewayService) doOpenAIUpstreamOnce(request *http.Request, proxyURL string, account *Account) (response *http.Response, resultErr error) {
	var timing *requesttiming.Attempt
	if recorder := requesttiming.FromContext(request.Context()); recorder != nil && request.Context().Value(openAIRequestTimingHTTPKey{}) == true {
		timing = recorder.Begin(account.ID, "http")
		if shape, ok := request.Context().Value(openAIRequestTimingShapeKey{}).(openAIRequestTimingShape); ok {
			timing.RequestShape(shape.anchor, shape.inputItems)
		}
		trace := &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { timing.Connection("", info.Reused, -1, -1, -1) },
			WroteRequest: func(info httptrace.WroteRequestInfo) {
				if info.Err == nil {
					timing.Sent(-1)
				}
			},
		}
		request = request.WithContext(httptrace.WithClientTrace(timing.Context(request.Context()), trace))
	}
	defer func() {
		if resultErr != nil {
			timing.Fail("transport_error")
		}
		if response != nil {
			timing.Headers(response.StatusCode, response.Header.Get("x-request-id"))
		}
	}()
	account = s.accountWithEgressTimezone(request.Context(), account, proxyURL)
	prepared, err := prepareCodexTimezoneRequest(request, account)
	if err != nil {
		return nil, err
	}
	if prepared != request {
		defer request.Body.Close()
		request = prepared
	}
	if request.Header.Get("Content-Encoding") == "" {
		timing.PayloadSize(request.ContentLength)
	} else {
		// A caller may already have encoded the body. Do not label its
		// compressed length as an observed JSON size.
		timing.EncodedSize(request.ContentLength)
	}
	timing.Stage("send_http")
	if s.pluginManager != nil {
		response, handled, err := s.pluginManager.RoundTripOpenAIOAuth(request.Context(), request, proxyURL, account)
		if handled {
			return response, err
		}
	}
	return doCodexEgressHTTP(s.cfg, s.httpUpstream, request, proxyURL, account)
}

// doOpenAIAccountTestUpstream 让 OpenAI OAuth 账号测试与真实转发使用同一插件路径。
// API Key 和未命中插件的账号保持各自原有的 HTTPUpstream 行为。
func (s *AccountTestService) doOpenAIAccountTestUpstream(
	request *http.Request,
	proxyURL string,
	account *Account,
	useTLSFallback bool,
) (*http.Response, error) {
	account = s.openaiGatewayService.accountWithEgressTimezone(request.Context(), account, proxyURL)
	prepared, err := prepareCodexTimezoneRequest(request, account)
	if err != nil {
		return nil, err
	}
	if prepared != request {
		defer request.Body.Close()
		request = prepared
	}
	// The explicit comparison profile fixes the transport and forbids fallback.
	if isComparisonRequest(request) {
		if !codexEgressEligible(s.cfg, account, request.URL, request.Header) {
			return nil, fmt.Errorf("comparison requires verified Codex HTTP transport; no fallback")
		}
		return doCodexEgressHTTP(s.cfg, s.httpUpstream, request, proxyURL, account)
	}
	if s.pluginManager != nil {
		response, handled, err := s.pluginManager.RoundTripOpenAIOAuth(request.Context(), request, proxyURL, account)
		if handled {
			return response, err
		}
	}
	if codexEgressEligible(s.cfg, account, request.URL, request.Header) {
		return doCodexEgressHTTP(s.cfg, s.httpUpstream, request, proxyURL, account)
	}
	if useTLSFallback {
		return s.httpUpstream.DoWithTLS(
			request,
			proxyURL,
			account.ID,
			account.Concurrency,
			s.tlsFPProfileService.ResolveTLSProfile(account),
		)
	}
	return s.httpUpstream.Do(request, proxyURL, account.ID, account.Concurrency)
}
