package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"

	"github.com/yourorg/go-clean-template/internal/domain"
	"github.com/yourorg/go-clean-template/internal/platform/metrics"
)

const (
	maxBackoff    = 5 * time.Second
	dlqRetryDelay = 2 * time.Second
)

// Processor applies a Handler to one record with the full reliability policy:
//
//	handler ok            -> done
//	transient error       -> retry with exponential backoff + jitter, up to MaxAttempts
//	permanent error       -> straight to the dead letter topic
//	attempts exhausted    -> dead letter topic
//
// Process only returns an error when ctx is cancelled. A record is never skipped silently: if the
// DLQ itself is unavailable it keeps retrying (blocking the partition) because losing the message
// would be worse than a pause.
type Processor struct {
	handler     Handler
	dlq         Publisher
	dlqTopic    string
	maxAttempts int
	backoff     time.Duration
	dlqRetry    time.Duration
	log         *slog.Logger
	metrics     *metrics.Metrics
}

func NewProcessor(h Handler, dlq Publisher, dlqTopic string, maxAttempts int, backoff time.Duration, log *slog.Logger, m *metrics.Metrics) *Processor {
	return &Processor{handler: h, dlq: dlq, dlqTopic: dlqTopic, maxAttempts: maxAttempts, backoff: backoff, dlqRetry: dlqRetryDelay, log: log, metrics: m}
}

func (p *Processor) Process(ctx context.Context, rec Record) error {
	// Continue the trace started by the producing request (propagated through the outbox row).
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(rec.Headers))
	ctx, span := otel.Tracer("messaging").Start(ctx, "consume "+rec.Topic)
	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination.name", rec.Topic),
		attribute.Int64("messaging.kafka.partition", int64(rec.Partition)),
		attribute.Int64("messaging.kafka.offset", rec.Offset),
	)
	defer span.End()

	var lastErr error
	for attempt := 1; attempt <= p.maxAttempts; attempt++ {
		lastErr = p.safeHandle(ctx, rec)
		if lastErr == nil {
			p.metrics.ConsumerMessages.WithLabelValues("ok").Inc()
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if isPermanent(lastErr) {
			break
		}
		p.metrics.ConsumerMessages.WithLabelValues("retry").Inc()
		p.log.WarnContext(ctx, "message handling failed, will retry",
			slog.String("topic", rec.Topic), slog.Int("partition", int(rec.Partition)), slog.Int64("offset", rec.Offset),
			slog.Int("attempt", attempt), slog.Any("error", lastErr))
		if attempt < p.maxAttempts {
			sleep(ctx, p.delay(attempt))
		}
	}

	span.RecordError(lastErr)
	span.SetStatus(codes.Error, "sent to dead letter topic")
	return p.deadLetter(ctx, rec, lastErr)
}

// safeHandle converts a handler panic into an error so one bad message cannot kill the consumer.
func (p *Processor) safeHandle(ctx context.Context, rec Record) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return p.handler(ctx, rec)
}

func (p *Processor) delay(attempt int) time.Duration {
	d := min(maxBackoff, p.backoff<<(attempt-1))
	jitter := time.Duration(rand.Int64N(int64(d)/5 + 1)) //nolint:gosec // jitter, not security
	return d + jitter
}

func (p *Processor) deadLetter(ctx context.Context, rec Record, cause error) error {
	hdrs := make(map[string]string, len(rec.Headers)+5)
	for k, v := range rec.Headers {
		hdrs[k] = v
	}
	hdrs["x-error"] = cause.Error()
	hdrs["x-original-topic"] = rec.Topic
	hdrs["x-original-partition"] = strconv.Itoa(int(rec.Partition))
	hdrs["x-original-offset"] = strconv.FormatInt(rec.Offset, 10)
	hdrs["x-failed-at"] = time.Now().UTC().Format(time.RFC3339)

	for {
		err := p.dlq.Produce(ctx, p.dlqTopic, []Message{{Key: string(rec.Key), Value: rec.Value, Headers: hdrs}})
		if err == nil {
			p.metrics.ConsumerMessages.WithLabelValues("dlq").Inc()
			p.log.ErrorContext(ctx, "message moved to dead letter topic",
				slog.String("topic", rec.Topic), slog.Int("partition", int(rec.Partition)),
				slog.Int64("offset", rec.Offset), slog.Any("cause", cause))
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		p.log.ErrorContext(ctx, "dead letter publish failed, retrying", slog.Any("error", err))
		sleep(ctx, p.dlqRetry)
	}
}

func isPermanent(err error) bool {
	return errors.Is(err, ErrPermanent) || errors.Is(err, domain.ErrInvalid)
}
