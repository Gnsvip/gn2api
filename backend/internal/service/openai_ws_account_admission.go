package service

import (
	"context"
	"time"

	coderws "github.com/coder/websocket"
)

// CheckOpenAIWSAccountTurn prevents a connection-scoped account snapshot from
// bypassing manual suspension or cooldowns. Admission is independent of traffic
// monitoring and never cancels a turn already sent to the upstream.
func (s *OpenAIGatewayService) CheckOpenAIWSAccountTurn(ctx context.Context, account *Account) error {
	if s == nil || account == nil || s.accountRepo == nil || !account.IsOpenAI() {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	latest, err := s.accountRepo.GetByID(ctx, account.ID)
	if err != nil {
		return NewOpenAIWSClientCloseError(coderws.StatusTryAgainLater, "unable to check account availability, please reconnect", err)
	}
	if latest == nil || !latest.IsSchedulable() {
		return NewOpenAIWSClientCloseError(coderws.StatusTryAgainLater, "account is no longer schedulable, please reconnect", nil)
	}
	return nil
}
