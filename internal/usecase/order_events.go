package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/yourorg/go-clean-template/internal/domain"
)

// OrderEventHandler reacts to order events consumed from the broker.
// In a real system this is where you would call an email or push provider; here it logs the
// notification and fans the update out to realtime subscribers.
type OrderEventHandler struct {
	realtime domain.Realtime
	log      *slog.Logger
}

func NewOrderEventHandler(rt domain.Realtime, log *slog.Logger) *OrderEventHandler {
	return &OrderEventHandler{realtime: rt, log: log}
}

// Handle must be idempotent-friendly: the transport guarantees at-least-once delivery and wraps
// this call in a processed_events claim, so a redelivered event is skipped.
// A payload that cannot be decoded returns domain.ErrInvalid, which the consumer treats as
// permanent (straight to the DLQ, no pointless retries).
func (h *OrderEventHandler) Handle(ctx context.Context, eventType string, payload []byte) error {
	switch eventType {
	case domain.EventOrderCreated, domain.EventOrderCancelled:
	default:
		h.log.DebugContext(ctx, "ignoring unknown event type", slog.String("type", eventType))
		return nil // forward compatible: new event types must not break old consumers
	}

	var e domain.OrderEvent
	if err := json.Unmarshal(payload, &e); err != nil {
		return fmt.Errorf("%w: decode %s payload: %w", domain.ErrInvalid, eventType, err)
	}

	h.log.InfoContext(ctx, "notify customer",
		slog.String("type", eventType),
		slog.String("order_id", e.OrderID.String()),
		slog.String("customer_id", e.CustomerID.String()),
		slog.String("status", string(e.Status)))

	if err := h.realtime.Publish(ctx, e.OrderID, payload); err != nil {
		return fmt.Errorf("publish realtime update: %w", err)
	}
	return nil
}
