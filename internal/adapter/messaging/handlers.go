package messaging

import (
	"context"
	"fmt"

	"github.com/yourorg/go-clean-template/internal/domain"
	"github.com/yourorg/go-clean-template/internal/usecase"
)

// Idempotent turns at-least-once delivery into exactly-once effects. It claims
// (consumer, event_id) and runs next inside the SAME database transaction: if next fails the claim
// rolls back and the retry runs again; if the event was already processed the claim fails and
// the record is skipped. Effects that live outside the database (HTTP calls, Redis publish) are
// still at-least-once, so keep them idempotent as well.
func Idempotent(consumer string, tx domain.TxManager, claims domain.EventClaimer, next Handler) Handler {
	return func(ctx context.Context, rec Record) error {
		eventID := rec.Header(headerEventID)
		if eventID == "" {
			return fmt.Errorf("%w: record without %s header", ErrPermanent, headerEventID)
		}
		return tx.WithinTx(ctx, func(ctx context.Context) error {
			first, err := claims.Claim(ctx, consumer, eventID)
			if err != nil {
				return fmt.Errorf("claim event %s: %w", eventID, err)
			}
			if !first {
				return nil // duplicate delivery
			}
			return next(ctx, rec)
		})
	}
}

// OrderEvents adapts the order use case to the generic record Handler.
func OrderEvents(h *usecase.OrderEventHandler) Handler {
	return func(ctx context.Context, rec Record) error {
		return h.Handle(ctx, rec.Header(headerEventType), rec.Value)
	}
}
