// Package messaging implements the broker side of the system on top of Kafka (franz-go):
//   - Producer: batched, idempotent, acks=all publisher;
//   - Relay: drains the transactional outbox to Kafka (producer side reliability);
//   - Consumer + Processor: parallel per partition, retries with backoff, DLQ, manual commits;
//   - Idempotent: exactly-once *effects* on top of at-least-once delivery.
package messaging

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Message is a record to produce.
type Message struct {
	Key     string
	Value   []byte
	Headers map[string]string
}

// Record is a consumed record, decoupled from the kgo type so handlers and tests need no Kafka.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
}

// Header returns a header value or "".
func (r Record) Header(k string) string { return r.Headers[k] }

// Publisher produces messages to a topic and returns only after the broker acknowledged all of them.
type Publisher interface {
	Produce(ctx context.Context, topic string, msgs []Message) error
}

// Handler processes one record. Returning an error triggers retries, see Processor.
type Handler func(ctx context.Context, rec Record) error

// ErrPermanent marks an error that retrying cannot fix (poison message). Wrap it with %w.
var ErrPermanent = errors.New("permanent processing error")

// Producer publishes with a shared franz-go client.
type Producer struct{ cl *kgo.Client }

var _ Publisher = (*Producer)(nil)

func NewProducer(cl *kgo.Client) *Producer { return &Producer{cl: cl} }

// Produce sends the batch and waits for acks from all in-sync replicas.
// Records with the same key always land in the same partition, which preserves per-key order.
func (p *Producer) Produce(ctx context.Context, topic string, msgs []Message) error {
	recs := make([]*kgo.Record, len(msgs))
	for i, m := range msgs {
		hdrs := make([]kgo.RecordHeader, 0, len(m.Headers))
		for k, v := range m.Headers {
			hdrs = append(hdrs, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		recs[i] = &kgo.Record{Topic: topic, Key: []byte(m.Key), Value: m.Value, Headers: hdrs}
	}
	if err := p.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}
	return nil
}

func toRecord(r *kgo.Record) Record {
	hdrs := make(map[string]string, len(r.Headers))
	for _, h := range r.Headers {
		hdrs[h.Key] = string(h.Value)
	}
	return Record{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value, Headers: hdrs}
}
