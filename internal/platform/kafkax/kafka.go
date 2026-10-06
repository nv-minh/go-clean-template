// Package kafkax builds franz-go clients with production defaults.
package kafkax

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/yourorg/go-clean-template/internal/platform/config"
)

func commonOpts(cfg config.Kafka) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DialTimeout(5 * time.Second),
		kgo.RequestRetries(5),
	}
	if cfg.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	if cfg.SASLMechanism != "" {
		var mech sasl.Mechanism
		auth := scram.Auth{User: cfg.SASLUser, Pass: cfg.SASLPassword}
		switch cfg.SASLMechanism {
		case "SCRAM-SHA-256":
			mech = auth.AsSha256Mechanism()
		case "SCRAM-SHA-512":
			mech = auth.AsSha512Mechanism()
		default:
			return nil, fmt.Errorf("unsupported KAFKA_SASL_MECHANISM %q", cfg.SASLMechanism)
		}
		opts = append(opts, kgo.SASL(mech))
	}
	return opts, nil
}

// NewProducer returns an idempotent, acks=all producer (franz-go enables idempotence by default).
// Batching (linger + compression) trades a few ms of latency for a large throughput gain.
func NewProducer(cfg config.Kafka, extra ...kgo.Opt) (*kgo.Client, error) {
	opts, err := commonOpts(cfg)
	if err != nil {
		return nil, err
	}
	opts = append(opts,
		kgo.ClientID(cfg.ClientID+"-producer"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.Lz4Compression(), kgo.SnappyCompression()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.RecordDeliveryTimeout(30*time.Second),
		kgo.RecordRetries(10),
	)
	opts = append(opts, extra...)
	return kgo.NewClient(opts...)
}

// NewConsumer returns a consumer group client with manual offset commits.
// BlockRebalanceOnPoll guarantees partitions are not revoked while records are being processed,
// which lets the consumer commit exactly what it has finished.
func NewConsumer(cfg config.Kafka, extra ...kgo.Opt) (*kgo.Client, error) {
	opts, err := commonOpts(cfg)
	if err != nil {
		return nil, err
	}
	opts = append(opts,
		kgo.ClientID(cfg.ClientID+"-consumer"),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(500*time.Millisecond),
		kgo.SessionTimeout(30*time.Second),
		kgo.RebalanceTimeout(60*time.Second),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	opts = append(opts, extra...)
	return kgo.NewClient(opts...)
}
