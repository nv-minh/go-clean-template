//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/yourorg/go-clean-template/internal/adapter/messaging"
	"github.com/yourorg/go-clean-template/internal/adapter/redisstore"
	"github.com/yourorg/go-clean-template/internal/adapter/repository"
	"github.com/yourorg/go-clean-template/internal/domain"
	"github.com/yourorg/go-clean-template/internal/platform/config"
	"github.com/yourorg/go-clean-template/internal/platform/kafkax"
	"github.com/yourorg/go-clean-template/internal/platform/metrics"
	"github.com/yourorg/go-clean-template/internal/usecase"
)

type received struct {
	eventID     string
	eventType   string
	orderID     uuid.UUID
	traceparent string
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", d, what)
}

// TestEventPipeline drives the real system: use case -> Postgres (+outbox) -> relay -> Kafka ->
// consumer -> idempotent handler, plus duplicate delivery, poison messages and trace propagation.
func TestEventPipeline(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	suffix := uuid.NewString()[:8]
	cfg := config.Kafka{
		Brokers: brokers, ClientID: "it", Topic: "orders.events." + suffix, DLQTopic: "orders.events.dlq." + suffix,
		ConsumerGroup: "it-" + suffix, MaxAttempts: 3, RetryBackoff: time.Millisecond, PollMaxRecs: 100,
	}
	createTopics(t, 3, cfg.Topic, cfg.DLQTopic)
	if _, err := pool.Exec(t.Context(), `TRUNCATE outbox`); err != nil {
		t.Fatal(err)
	}

	log := slog.New(slog.DiscardHandler)
	m := metrics.New("it")
	producerCl, err := kafkax.NewProducer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producerCl.Close)
	consumerCl, err := kafkax.NewConsumer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(consumerCl.Close)

	store := repository.NewStore(pool)
	outbox := repository.NewOutbox(store)
	claims := repository.NewProcessedEvents(store)
	svc := usecase.NewOrderService(repository.NewOrderRepository(store), outbox, store,
		redisstore.NewOrderCache(rdb, time.Minute, m), log)

	var (
		mu  sync.Mutex
		got []received
	)
	snapshot := func() []received {
		mu.Lock()
		defer mu.Unlock()
		return append([]received(nil), got...)
	}
	handler := messaging.Idempotent(cfg.ConsumerGroup, store, claims, func(_ context.Context, rec messaging.Record) error {
		var e domain.OrderEvent
		if err := json.Unmarshal(rec.Value, &e); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		got = append(got, received{rec.Header("event_id"), rec.Header("event_type"), e.OrderID, rec.Header("traceparent")})
		return nil
	})
	producer := messaging.NewProducer(producerCl)
	processor := messaging.NewProcessor(handler, producer, cfg.DLQTopic, cfg.MaxAttempts, cfg.RetryBackoff, log, m)

	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	relay := messaging.NewRelay(outbox, producer, cfg.Topic,
		config.Outbox{BatchSize: 50, PollInterval: 20 * time.Millisecond, Retention: time.Hour, CleanupInterval: time.Hour}, log, m)
	consumer := messaging.NewConsumer(consumerCl, processor, cfg.PollMaxRecs, log)
	wg.Go(func() { _ = relay.Run(ctx) })
	wg.Go(func() { _ = consumer.Run(ctx) })

	// 1. Business writes produce exactly one event each, delivered through Kafka.
	spanCtx, span := otel.Tracer("it").Start(t.Context(), "request")
	wantTrace := span.SpanContext().TraceID().String()
	var orders []*domain.Order
	for range 20 {
		o, err := svc.Create(spanCtx, usecase.CreateOrderInput{
			CustomerID: uuid.New(), Currency: "USD", Items: []domain.Item{{SKU: "A", Quantity: 1, UnitPrice: 10}},
		})
		if err != nil {
			t.Fatal(err)
		}
		orders = append(orders, o)
	}
	span.End()
	waitFor(t, 30*time.Second, "20 order.created events", func() bool { return len(snapshot()) == 20 })

	// 2. Per key ordering: cancelling emits order.cancelled strictly after order.created.
	if _, err := svc.Cancel(t.Context(), orders[0].ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, "order.cancelled event", func() bool { return len(snapshot()) == 21 })
	var types []string
	for _, r := range snapshot() {
		if r.orderID == orders[0].ID {
			types = append(types, r.eventType)
		}
	}
	if strings.Join(types, ",") != "order.created,order.cancelled" {
		t.Fatalf("per aggregate order violated: %v", types)
	}

	// 3. Trace context set at write time reaches the consumer through the outbox and Kafka headers.
	for _, r := range snapshot()[:20] {
		if !strings.Contains(r.traceparent, wantTrace) {
			t.Fatalf("traceparent %q does not carry trace id %s", r.traceparent, wantTrace)
		}
	}

	// 4. Redelivery (relay crash after publish, before commit) must not repeat effects.
	if _, err := pool.Exec(t.Context(), `UPDATE outbox SET published_at = NULL`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, "outbox republished", func() bool {
		var n int
		_ = pool.QueryRow(t.Context(), `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
		return n == 0
	})
	time.Sleep(2 * time.Second) // give the consumer time to (wrongly) process duplicates
	if n := len(snapshot()); n != 21 {
		t.Fatalf("duplicates were processed: %d events recorded, want 21", n)
	}

	// 5. A poison message ends up in the DLQ with diagnostics and does not block the partition.
	if err := producer.Produce(t.Context(), cfg.Topic, []messaging.Message{{Key: "poison", Value: []byte("not json"), Headers: map[string]string{"event_id": "poison-1"}}}); err != nil {
		t.Fatal(err)
	}
	dlqCl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(cfg.DLQTopic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dlqCl.Close)
	pollCtx, pollCancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer pollCancel()
	var dead *kgo.Record
	for dead == nil && pollCtx.Err() == nil {
		dlqCl.PollFetches(pollCtx).EachRecord(func(r *kgo.Record) { dead = r })
	}
	if dead == nil || string(dead.Value) != "not json" {
		t.Fatalf("poison message not found in DLQ: %+v", dead)
	}
	hdrs := map[string]string{}
	for _, h := range dead.Headers {
		hdrs[h.Key] = string(h.Value)
	}
	if hdrs["x-original-topic"] != cfg.Topic || hdrs["x-error"] == "" {
		t.Fatalf("DLQ record lacks diagnostics: %v", hdrs)
	}
	svcOrder, err := svc.Create(t.Context(), usecase.CreateOrderInput{CustomerID: uuid.New(), Currency: "USD", Items: []domain.Item{{SKU: "Z", Quantity: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, "event after poison message", func() bool {
		for _, r := range snapshot() {
			if r.orderID == svcOrder.ID {
				return true
			}
		}
		return false
	})
}
