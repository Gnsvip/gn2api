package handler

import (
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func (h *OpenAIGatewayHandler) configureCompactionBilling(c *gin.Context, key *service.APIKey, account *service.Account, subscription *service.UserSubscription, fields service.ChannelUsageFields, pricingAt time.Time) {
	service.SetOpenAICompactionBilling(c, &service.OpenAIRecordUsageInput{
		APIKey: key, User: key.User, Account: account, Subscription: subscription,
		InboundEndpoint: GetInboundEndpoint(c), UserAgent: c.GetHeader("User-Agent"),
		IPAddress: ip.GetClientIP(c), SessionID: service.ExtractClientSessionID(c),
		APIKeyService: h.apiKeyService, QuotaPlatform: service.QuotaPlatform(c.Request.Context(), key),
		PricingAt: pricingAt, ChannelUsageFields: fields,
	})
}
