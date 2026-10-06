package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/yourorg/go-clean-template/internal/adapter/repository/sqlcdb"
	"github.com/yourorg/go-clean-template/internal/domain"
)

// outboxLockKey is the arbitrary advisory lock id that elects the single active relay.
const outboxLockKey int64 = 0x6f757462 // "outb"

// Outbox implements domain.OutboxWriter and domain.OutboxStore.
type Outbox struct{ store *Store }

var (
	_ domain.OutboxWriter = (*Outbox)(nil)
	_ domain.OutboxStore  = (*Outbox)(nil)
)

func NewOutbox(store *Store) *Outbox { return &Outbox{store: store} }

// Append inserts the event using the ambient transaction. Call it inside TxManager.WithinTx,
// otherwise the event is not atomic with the business change and the pattern is defeated.
// The current trace context is stored with the row so the trace continues through Kafka.
func (o *Outbox) Append(ctx context.Context, e domain.Event) error {
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	headers, err := json.Marshal(carrier)
	if err != nil {
		return fmt.Errorf("marshal event headers: %w", err)
	}
	return mapErr(o.store.queries(ctx).InsertOutboxEvent(ctx, sqlcdb.InsertOutboxEventParams{
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID,
		EventType:     e.Type,
		Payload:       payload,
		Headers:       headers,
	}))
}

func (o *Outbox) ProcessBatch(ctx context.Context, limit int, fn func(ctx context.Context, events []domain.OutboxEvent) error) (int, error) {
	tx, err := o.store.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			rollback(tx)
		}
	}()
	q := sqlcdb.New(tx)

	locked, err := q.TryOutboxLock(ctx, outboxLockKey)
	if err != nil {
		return 0, fmt.Errorf("outbox lock: %w", err)
	}
	if !locked {
		return 0, nil // another replica is the active relay
	}

	rows, err := q.ClaimOutboxBatch(ctx, int32(limit)) //nolint:gosec // limit comes from validated config
	if err != nil {
		return 0, fmt.Errorf("claim outbox batch: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	events := make([]domain.OutboxEvent, len(rows))
	ids := make([]int64, len(rows))
	for i, r := range rows {
		var headers map[string]string
		if len(r.Headers) > 0 {
			if err := json.Unmarshal(r.Headers, &headers); err != nil {
				return 0, fmt.Errorf("unmarshal headers of outbox event %d: %w", r.ID, err)
			}
		}
		events[i] = domain.OutboxEvent{
			ID: r.ID, AggregateType: r.AggregateType, AggregateID: r.AggregateID,
			EventType: r.EventType, Payload: r.Payload, Headers: headers, CreatedAt: r.CreatedAt,
		}
		ids[i] = r.ID
	}

	if err := fn(ctx, events); err != nil {
		return 0, err
	}
	if err := q.MarkOutboxPublished(ctx, ids); err != nil {
		return 0, fmt.Errorf("mark outbox published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit outbox batch: %w", err)
	}
	committed = true
	return len(events), nil
}

func (o *Outbox) DeletePublishedBefore(ctx context.Context, t time.Time) (int64, error) {
	n, err := o.store.queries(ctx).DeletePublishedOutbox(ctx, t)
	return n, mapErr(err)
}

// ProcessedEvents implements domain.EventClaimer.
type ProcessedEvents struct{ store *Store }

var _ domain.EventClaimer = (*ProcessedEvents)(nil)

func NewProcessedEvents(store *Store) *ProcessedEvents { return &ProcessedEvents{store: store} }

func (p *ProcessedEvents) Claim(ctx context.Context, consumer, eventID string) (bool, error) {
	n, err := p.store.queries(ctx).ClaimProcessedEvent(ctx, sqlcdb.ClaimProcessedEventParams{Consumer: consumer, EventID: eventID})
	if err != nil {
		return false, mapErr(err)
	}
	return n == 1, nil
}
