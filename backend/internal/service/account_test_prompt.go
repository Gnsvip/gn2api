package service

import (
	"strings"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
)

const attributionTestContextKey = "account_test_attribution"

// IsAttributionTestMode keeps monitoring separate from manual account recovery.
func IsAttributionTestMode(mode string) bool {
	return strings.EqualFold(strings.TrimSpace(mode), "attribution")
}

func isAttributionProbe(c *gin.Context) bool {
	return c.GetBool(attributionTestContextKey)
}

func accountTestPrompt(prompts []string) string {
	if len(prompts) > 0 && strings.TrimSpace(prompts[0]) != "" {
		return prompts[0]
	}
	return "hi"
}

func validAttributionPrompt(model, prompt string) bool {
	return strings.TrimSpace(model) != "" && utf8.RuneCountInString(model) <= 160 &&
		strings.TrimSpace(prompt) != "" && utf8.RuneCountInString(prompt) <= 6000
}
