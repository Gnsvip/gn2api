package service

import (
	"context"
	"time"
)

type openAIWSConnectDeadlineKey struct{}

// Carry an acquisition deadline as a value, NOT a deadline on the response
// context: model reasoning and streamed output must outlive the dial budget.
func withOpenAIWSConnectBudget(ctx context.Context, start time.Time, budget time.Duration) context.Context {
	if budget <= 0 {
		return ctx
	}
	return context.WithValue(ctx, openAIWSConnectDeadlineKey{}, start.Add(budget))
}

func (s *OpenAIGatewayService) newOpenAIWSAcquireContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(s.openAIWSAcquireTimeout())
	if budgetDeadline, ok := ctx.Value(openAIWSConnectDeadlineKey{}).(time.Time); ok && budgetDeadline.Before(deadline) {
		deadline = budgetDeadline
	}
	return context.WithDeadline(ctx, deadline)
}
