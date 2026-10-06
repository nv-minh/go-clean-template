// Package domain holds the enterprise business rules: entities, value objects, domain errors
// and the ports (interfaces) the outer layers must implement.
// It imports nothing from this module and no third party framework (enforced by depguard).
package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Sentinel errors. The HTTP layer maps them to status codes; use errors.Is to test.
var (
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("conflict")
	ErrInvalid      = errors.New("invalid input")
	ErrInvalidState = errors.New("invalid state transition")
	ErrCacheMiss    = errors.New("cache miss")
)

type Status string

const (
	StatusPending   Status = "PENDING"
	StatusPaid      Status = "PAID"
	StatusCancelled Status = "CANCELLED"
)

const (
	maxItems    = 100
	maxQuantity = 10_000
)

// Item is one order line. Money is always integer minor units (cents) to avoid float errors.
type Item struct {
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	UnitPrice int64  `json:"unit_price"`
}

// Order is the aggregate root.
type Order struct {
	ID          uuid.UUID
	CustomerID  uuid.UUID
	Status      Status
	Currency    string
	TotalAmount int64
	Items       []Item
	// Version enables optimistic concurrency control: an update only succeeds if the row still
	// has the version this aggregate was loaded with.
	Version   int32
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewOrder validates the input and builds a PENDING order. IDs are UUIDv7 (time ordered), which
// keeps B-tree inserts append-mostly and index pages hot, unlike random UUIDv4.
func NewOrder(customerID uuid.UUID, currency string, items []Item, now time.Time) (*Order, error) {
	if customerID == uuid.Nil {
		return nil, fmt.Errorf("%w: customer_id is required", ErrInvalid)
	}
	currency = strings.ToUpper(strings.TrimSpace(currency))
	if len(currency) != 3 {
		return nil, fmt.Errorf("%w: currency must be a 3 letter ISO code", ErrInvalid)
	}
	if len(items) == 0 || len(items) > maxItems {
		return nil, fmt.Errorf("%w: items must contain between 1 and %d lines", ErrInvalid, maxItems)
	}

	var total int64
	for i, it := range items {
		if strings.TrimSpace(it.SKU) == "" {
			return nil, fmt.Errorf("%w: items[%d].sku is required", ErrInvalid, i)
		}
		if it.Quantity < 1 || it.Quantity > maxQuantity {
			return nil, fmt.Errorf("%w: items[%d].quantity must be between 1 and %d", ErrInvalid, i, maxQuantity)
		}
		if it.UnitPrice < 0 {
			return nil, fmt.Errorf("%w: items[%d].unit_price must not be negative", ErrInvalid, i)
		}
		total += int64(it.Quantity) * it.UnitPrice
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate order id: %w", err)
	}
	return &Order{
		ID:          id,
		CustomerID:  customerID,
		Status:      StatusPending,
		Currency:    currency,
		TotalAmount: total,
		Items:       items,
		Version:     1,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

// Cancel moves a PENDING order to CANCELLED. Other states are final for this operation.
func (o *Order) Cancel(now time.Time) error {
	if o.Status != StatusPending {
		return fmt.Errorf("%w: cannot cancel an order in status %s", ErrInvalidState, o.Status)
	}
	o.Status = StatusCancelled
	o.UpdatedAt = now
	return nil
}
