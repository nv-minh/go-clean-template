package messaging

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/yourorg/go-clean-template/internal/domain"
	"github.com/yourorg/go-clean-template/internal/platform/config"
	"github.com/yourorg/go-clean-template/internal/platform/metrics"
)

const (
	headerEventID       = "event_id"
	headerEventType     = "event_type"
	headerAggregateType = "aggregate_type"
	headerContentType   = "content-type"

	maxErrorBackoff = 5 * time.Second
)

// Relay moves events from the outbox table to Kafka.
//
// Delivery guarantee is AT LEAST ONCE: if the process dies after Kafka acknowledged a batch but
// before the database commit, the batch is published again. Consumers must therefore be
// idempotent (see Idempotent). In exchange an event is never lost and never published for a
// transaction that rolled back, which is impossible with "write DB then publish" dual writes.
//
// Many replicas can run; a Postgres advisory lock lets exactly one drain at a time, which keeps
// events strictly ordered. The others idle as hot standbys.
type Relay struct {
	store   domain.OutboxStore
	pub     Publisher
	topic   string
	cfg     config.Outbox
	log     *slog.Logger
	metrics *metrics.Metrics
}

func NewRelay(store domain.OutboxStore, pub Publisher, topic string, cfg config.Outbox, log *slog.Logger, m *metrics.Metrics) *Relay {
	return &Relay{store: store, pub: pub, topic: topic, cfg: cfg, log: log, metrics: m}
}

// Run blocks until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	r.log.Info("outbox relay started", slog.String("topic", r.topic), slog.Int("batch_size", r.cfg.BatchSize))
	var (
		failures    int
		lastCleanup = time.Now()
	)
	for ctx.Err() == nil {
		n, err := r.drainOnce(ctx)
		switch {
		case err != nil:
			failures++
			wait := min(maxErrorBackoff, 100*time.Millisecond<<min(failures, 6))
			r.log.ErrorContext(ctx, "outbox drain failed", slog.Any("error", err), slog.Duration("retry_in", wait))
			sleep(ctx, wait)
		case n >= r.cfg.BatchSize:
			failures = 0 // backlog: loop again immediately
		default:
			failures = 0
			sleep(ctx, r.cfg.PollInterval)
		}

		if time.Since(lastCleanup) >= r.cfg.CleanupInterval {
			lastCleanup = time.Now()
			r.cleanup(ctx)
		}
	}
	r.log.Info("outbox relay stopped")
	return nil
}

func (r *Relay) drainOnce(ctx context.Context) (int, error) {
	start := time.Now()
	n, err := r.store.ProcessBatch(ctx, r.cfg.BatchSize, func(ctx context.Context, events []domain.OutboxEvent) error {
		msgs := make([]Message, len(events))
		for i, e := range events {
			hdrs := make(map[string]string, len(e.Headers)+4)
			for k, v := range e.Headers { // includes W3C traceparent captured at write time
				hdrs[k] = v
			}
			hdrs[headerEventID] = strconv.FormatInt(e.ID, 10)
			hdrs[headerEventType] = e.EventType
			hdrs[headerAggregateType] = e.AggregateType
			hdrs[headerContentType] = "application/json"
			msgs[i] = Message{Key: e.AggregateID, Value: e.Payload, Headers: hdrs}
		}
		return r.pub.Produce(ctx, r.topic, msgs)
	})
	if err == nil && n > 0 {
		r.metrics.OutboxPublished.Add(float64(n))
		r.metrics.OutboxBatchSeconds.Observe(time.Since(start).Seconds())
	}
	return n, err
}

// cleanup deletes published rows past retention so the table (and its indexes) stay small.
// At very high volume, replace this with time based table partitioning and DROP PARTITION.
func (r *Relay) cleanup(ctx context.Context) {
	n, err := r.store.DeletePublishedBefore(ctx, time.Now().Add(-r.cfg.Retention))
	if err != nil {
		r.log.WarnContext(ctx, "outbox cleanup failed", slog.Any("error", err))
		return
	}
	if n > 0 {
		r.log.InfoContext(ctx, "outbox cleanup", slog.Int64("deleted", n))
	}
}

// sleep waits for d or until ctx is done.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
