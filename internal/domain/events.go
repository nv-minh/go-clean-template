package domain

import (
	"time"

	"github.com/google/uuid"
)

const (
	AggregateOrder = "order"

	EventOrderCreated   = "order.created"
	EventOrderCancelled = "order.cancelled"
)

// OrderEvent is the payload published for every order state change.
// It is a public contract for downstream consumers: only add fields, never rename or remove.
type OrderEvent struct {
	OrderID     uuid.UUID `json:"order_id"`
	CustomerID  uuid.UUID `json:"customer_id"`
	Status      Status    `json:"status"`
	Currency    string    `json:"currency"`
	TotalAmount int64     `json:"total_amount"`
	OccurredAt  time.Time `json:"occurred_at"`
}

// NewOrderEvent builds the outbox event for an order.
func NewOrderEvent(eventType string, o *Order, at time.Time) Event {
	return Event{
		Type:          eventType,
		AggregateType: AggregateOrder,
		AggregateID:   o.ID.String(),
		Payload: OrderEvent{
			OrderID:     o.ID,
			CustomerID:  o.CustomerID,
			Status:      o.Status,
			Currency:    o.Currency,
			TotalAmount: o.TotalAmount,
			OccurredAt:  at,
		},
	}
}
