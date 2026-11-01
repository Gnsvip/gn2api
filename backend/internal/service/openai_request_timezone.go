package service

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

// openAIRequestTimezoneExtraKey 是旧版保存的账号请求时区键。
// 保留配置兼容性；实际出站时区由出口 IP 缓存决定，不采用账号覆盖值。
const openAIRequestTimezoneExtraKey = "openai_request_timezone"

// DefaultOpenAIRequestTimezone 是拿不到出口时区时的回退值。
// Unknown egress retains the existing UTC fallback and is explicitly marked; it is not a verified IP/timezone match.
const DefaultOpenAIRequestTimezone = codexDefaultTimezone

// directEgressProxyID 是「无代理、直连出口」在 proxy:latency 缓存里的 sentinel 键。
// 真实代理 ID 都是正数，负数不会和数据库自增冲突。
// 决定 4：缓存按「代理 / 直连」分键，不按账号分键——同一个代理下的多个账号
// 共享同一个出口时区，直连账号共享直连出口的时区。
const directEgressProxyID int64 = -1

// OpenAIRequestTimezone 读取旧版保存的账号时区（须为合法 IANA 时区）。
// 此值不代表当前出口时区；出站调用方必须使用解析器。
func (a *Account) OpenAIRequestTimezone() string {
	if a == nil || !a.IsOpenAI() {
		return ""
	}
	return NormalizeOpenAIRequestTimezone(a.GetExtraString(openAIRequestTimezoneExtraKey))
}

// NormalizeOpenAIRequestTimezone 校验并规整一个 IANA 时区名；非法返回空串。
func NormalizeOpenAIRequestTimezone(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	if _, err := time.LoadLocation(name); err != nil {
		return ""
	}
	return name
}

// OpenAIRequestTimezoneResolution 是一次时区解析的结果。
type OpenAIRequestTimezoneResolution struct {
	Timezone string
	// Source 记录时区来源：proxy=代理出口缓存 / direct=直连出口缓存 /
	// fallback=未取得可信出口时区，回退默认值。
	Source   string
	Fallback bool
}

// OpenAIRequestTimezoneResolver 按出口 IP 自动确定 OpenAI 账号的请求时区。
//
// 有代理看代理出口，没代理看服务器直连出口；缓存按「代理 / 直连」分键。
// 旧账号时区不参与选择；拿不到可信出口时区时使用默认值并标记为回退。
type OpenAIRequestTimezoneResolver struct {
	latencyCache    ProxyLatencyCache
	defaultTimezone string
}

func NewOpenAIRequestTimezoneResolver(latencyCache ProxyLatencyCache, defaultTimezone string) *OpenAIRequestTimezoneResolver {
	if NormalizeOpenAIRequestTimezone(defaultTimezone) == "" {
		defaultTimezone = DefaultOpenAIRequestTimezone
	}
	return &OpenAIRequestTimezoneResolver{
		latencyCache:    latencyCache,
		defaultTimezone: defaultTimezone,
	}
}

// Resolve 决定一个账号出站请求应该使用的时区。
// 只查询实际代理（或无代理时的服务器直连）缓存；无效时回退默认，
// 不能借用另一个出口的缓存，也不能恢复旧账号手动设置。
func (r *OpenAIRequestTimezoneResolver) Resolve(ctx context.Context, account *Account) OpenAIRequestTimezoneResolution {
	if account == nil || !account.IsOpenAI() {
		return OpenAIRequestTimezoneResolution{Timezone: r.defaultTimezone, Source: "fallback", Fallback: true}
	}
	cacheKey := directEgressProxyID
	if account.ProxyID != nil && *account.ProxyID > 0 {
		cacheKey = *account.ProxyID
	}
	if r.latencyCache != nil {
		if latencies, err := r.latencyCache.GetProxyLatencies(ctx, []int64{cacheKey}); err == nil {
			if info := latencies[cacheKey]; info != nil && info.Success && net.ParseIP(info.IPAddress) != nil && !info.UpdatedAt.IsZero() && time.Since(info.UpdatedAt) <= 2*time.Hour && info.EgressFingerprint == codexEgressFingerprint(accountProxyURL(account)) {
				if tz := NormalizeOpenAIRequestTimezone(info.Timezone); tz != "" {
					source := "direct"
					if cacheKey != directEgressProxyID {
						source = "proxy"
					}
					return OpenAIRequestTimezoneResolution{Timezone: tz, Source: source}
				}
			}
		}
	}
	logger.FromContext(ctx).Debug("openai request timezone fallback",
		zap.Int64("account_id", account.ID), zap.Int64("egress_cache_key", cacheKey),
		zap.String("default_timezone", r.defaultTimezone))
	return OpenAIRequestTimezoneResolution{Timezone: r.defaultTimezone, Source: "fallback", Fallback: true}
}

func accountProxyURL(account *Account) string {
	if account != nil && account.ProxyID != nil && *account.ProxyID > 0 && account.Proxy != nil {
		return account.Proxy.URL()
	}
	return ""
}
func codexEgressFingerprint(proxyURL string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(proxyURL)))
}

// Return a request-owned snapshot. Never mutate scheduler/account-cache state.
func (s *OpenAIGatewayService) accountWithEgressTimezone(ctx context.Context, account *Account, proxyURL string) *Account {
	if s == nil || s.requestTimezoneResolver == nil || !isCodexOAuthAccount(account) {
		return account
	}
	selected := *account
	resolutionAccount := &selected
	// Actual transport selection is authoritative. Do not apply another proxy's cached geolocation.
	if proxyURL == "" {
		selected.ProxyID = nil
		selected.Proxy = nil
	} else if proxyURL != accountProxyURL(account) {
		// An unidentified proxy has no verified cache association. Use the
		// explicit fallback, never the account's old timezone or direct IP.
		resolutionAccount = nil
	}
	resolution := s.requestTimezoneResolver.Resolve(ctx, resolutionAccount)
	selected.Extra = shallowCopyMap(account.Extra)
	if selected.Extra == nil {
		selected.Extra = map[string]any{}
	}
	selected.Extra[codexTimezoneExtraKey] = resolution.Timezone
	selected.Extra["codex_timezone_source"] = resolution.Source
	return &selected
}
