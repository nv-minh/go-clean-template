package messaging

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer polls a consumer group and feeds records to a Processor.
//
// Parallelism: one goroutine per assigned partition per poll (partitions are independent), while
// records inside a partition are handled strictly in order. Scale out by adding worker replicas
// up to the partition count; beyond that extra replicas sit idle.
//
// Commits are manual and happen only after the batch was fully processed (or dead lettered),
// so a crash replays at most one poll worth of records: at-least-once delivery.
type Consumer struct {
	cl      *kgo.Client
	proc    *Processor
	pollMax int
	log     *slog.Logger
}

func NewConsumer(cl *kgo.Client, proc *Processor, pollMax int, log *slog.Logger) *Consumer {
	return &Consumer{cl: cl, proc: proc, pollMax: pollMax, log: log}
}

// Run blocks until ctx is cancelled or the client is closed.
func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("consumer started")
	for {
		fetches := c.cl.PollRecords(ctx, c.pollMax)
		if fetches.IsClientClosed() {
			return nil
		}
		if ctx.Err() != nil {
			c.cl.AllowRebalance()
			c.log.Info("consumer stopped")
			return nil //nolint:nilerr // shutdown requested: the poll error is just the cancellation
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.Error("fetch error", slog.String("topic", topic), slog.Int("partition", int(partition)), slog.Any("error", err))
		})

		var (
			wg   sync.WaitGroup
			mu   sync.Mutex
			last []*kgo.Record // last fully processed record of each partition
		)
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				var done *kgo.Record
				for _, r := range p.Records {
					if err := c.proc.Process(ctx, toRecord(r)); err != nil {
						break // only on shutdown: commit what we finished
					}
					done = r
				}
				if done != nil {
					mu.Lock()
					last = append(last, done)
					mu.Unlock()
				}
			}()
		})
		wg.Wait()

		if len(last) > 0 {
			// Detached context: on shutdown we still want to persist the progress we made.
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			if err := c.cl.CommitRecords(cctx, last...); err != nil {
				c.log.Error("offset commit failed (records may be redelivered)", slog.Any("error", err))
			}
			cancel()
		}
		c.cl.AllowRebalance() // required because of BlockRebalanceOnPoll
	}
}
