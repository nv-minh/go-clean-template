package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Cursor is the keyset pagination position: the (created_at, id) of the last row of a page.
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

type ListParams struct {
	CustomerID uuid.UUID
	Cursor     *Cursor // nil means first page
	Limit      int
}

// OrderRepository persists orders. Implementations must participate in the transaction carried by
// the context when TxManager.WithinTx is active.
type OrderRepository interface {
	Create(ctx context.Context, o *Order) error
	GetByID(ctx context.Context, id uuid.UUID) (*Order, error)
	// List returns orders newest first using keyset pagination.
	List(ctx context.Context, p ListParams) ([]*Order, error)
	// UpdateStatus applies o.Status guarded by o.Version and bumps o.Version on success.
	// It returns ErrConflict when the stored version differs.
	UpdateStatus(ctx context.Context, o *Order) error
}

// TxManager runs fn atomically. Nested calls join the outer transaction.
type TxManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// Event is a domain event to publish. Appending it through OutboxWriter inside the same
// transaction as the state change is what guarantees it is never lost nor emitted for a
// rolled back change (transactional outbox).
type Event struct {
	Type          string
	AggregateType string
	AggregateID   string
	Payload       any
}

type OutboxWriter interface {
	Append(ctx context.Context, e Event) error
}

// OutboxEvent is a persisted, not yet (or already) published event.
type OutboxEvent struct {
	ID            int64
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       []byte
	Headers       map[string]string // trace context and other transport metadata
	CreatedAt     time.Time
}

// OutboxStore is used by the relay to drain the outbox.
type OutboxStore interface {
	// ProcessBatch atomically claims up to limit unpublished events in id order, calls fn, and
	// marks them published only if fn returns nil. It returns the number of events handled.
	// Only one caller across all replicas works at a time, which preserves global ordering.
	ProcessBatch(ctx context.Context, limit int, fn func(ctx context.Context, events []OutboxEvent) error) (int, error)
	// DeletePublishedBefore removes already published rows older than t.
	DeletePublishedBefore(ctx context.Context, t time.Time) (int64, error)
}

// EventClaimer gives consumers exactly-once effects on top of at-least-once delivery.
type EventClaimer interface {
	// Claim records (consumer, eventID). It returns false if it was already recorded.
	// Call it inside the same transaction as the side effect it protects.
	Claim(ctx context.Context, consumer, eventID string) (bool, error)
}

// OrderCache is a read-through cache for orders. Get returns ErrCacheMiss when absent.
// All methods are best effort: callers must treat any other error as a miss.
type OrderCache interface {
	Get(ctx context.Context, id uuid.UUID) (*Order, error)
	Set(ctx context.Context, o *Order) error
	Delete(ctx context.Context, id uuid.UUID) error
}

// Realtime fans an update out to everyone currently watching an order (SSE clients on any replica).
type Realtime interface {
	Publish(ctx context.Context, orderID uuid.UUID, payload []byte) error
}
