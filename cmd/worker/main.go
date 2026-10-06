// Command worker runs the asynchronous side of the system: the outbox relay (DB -> Kafka) and the
// Kafka consumer. It is separate from the API so each scales independently: API on request rate,
// worker on queue depth / consumer lag.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"golang.org/x/sync/errgroup"

	"github.com/yourorg/go-clean-template/internal/adapter/messaging"
	"github.com/yourorg/go-clean-template/internal/adapter/redisstore"
	"github.com/yourorg/go-clean-template/internal/adapter/repository"
	"github.com/yourorg/go-clean-template/internal/platform/bootstrap"
	"github.com/yourorg/go-clean-template/internal/platform/kafkax"
	"github.com/yourorg/go-clean-template/internal/usecase"
)

var version = "dev" // set with -ldflags "-X main.version=..."

func main() {
	bootstrap.HandleHealthcheckCommand()
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := bootstrap.SignalContext()
	defer stop()

	rt, err := bootstrap.New(ctx, "worker")
	if err != nil {
		return err
	}
	defer func() { _ = rt.Close(context.Background()) }()
	cfg, log := rt.Cfg, rt.Log
	log.Info("starting worker", slog.String("version", version))

	// One shared hook exports franz-go client metrics (produce/fetch latency, bytes, errors,
	// buffered records, consumer lag as seen by the client) labelled per client id.
	kafkaMetrics := kprom.NewMetrics("app", kprom.Subsystem("kafka"), kprom.Registry(rt.Metrics.Registry), kprom.WithClientLabel())
	hooks := kgo.WithHooks(kafkaMetrics)

	producerClient, err := kafkax.NewProducer(cfg.Kafka, hooks)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	rt.AddCloser("kafka-producer", func(ctx context.Context) error {
		err := producerClient.Flush(ctx)
		producerClient.Close()
		return err
	})
	consumerClient, err := kafkax.NewConsumer(cfg.Kafka, hooks)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	rt.AddCloser("kafka-consumer", func(context.Context) error { consumerClient.Close(); return nil })
	rt.Health.Add("kafka", true, producerClient.Ping)

	store := repository.NewStore(rt.Pool)
	outbox := repository.NewOutbox(store)
	processed := repository.NewProcessedEvents(store)
	realtime := redisstore.NewRealtime(rt.Redis, log)
	producer := messaging.NewProducer(producerClient)

	handler := messaging.Idempotent(cfg.Kafka.ConsumerGroup, store, processed,
		messaging.OrderEvents(usecase.NewOrderEventHandler(realtime, log)))
	processor := messaging.NewProcessor(handler, producer, cfg.Kafka.DLQTopic,
		cfg.Kafka.MaxAttempts, cfg.Kafka.RetryBackoff, log, rt.Metrics)

	relay := messaging.NewRelay(outbox, producer, cfg.Kafka.Topic, cfg.Outbox, log, rt.Metrics)
	consumer := messaging.NewConsumer(consumerClient, processor, cfg.Kafka.PollMaxRecs, log)

	stopOps := rt.StartOps()
	defer stopOps()

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return relay.Run(gctx) })
	g.Go(func() error { return consumer.Run(gctx) })
	err = g.Wait()
	rt.Health.MarkShuttingDown()
	return err
}
