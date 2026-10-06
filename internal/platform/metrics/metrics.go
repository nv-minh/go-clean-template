// Package metrics owns the Prometheus registry and the application-level collectors.
// Collectors are plain fields so tests can build an isolated instance with New.
package metrics

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Registry *prometheus.Registry

	HTTPRequests *prometheus.CounterVec
	HTTPDuration *prometheus.HistogramVec
	HTTPInFlight prometheus.Gauge

	CacheOps *prometheus.CounterVec

	OutboxPublished    prometheus.Counter
	OutboxBatchSeconds prometheus.Histogram

	ConsumerMessages *prometheus.CounterVec
}

func New(namespace string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		Registry: reg,
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "http_requests_total", Help: "HTTP requests by method, route pattern and status.",
		}, []string{"method", "route", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "http_request_duration_seconds", Help: "HTTP request latency.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"method", "route"}),
		HTTPInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "http_in_flight_requests", Help: "Requests currently being served.",
		}),
		CacheOps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "cache_operations_total", Help: "Cache operations by result (hit, miss, error, skipped).",
		}, []string{"result"}),
		OutboxPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "outbox_published_total", Help: "Outbox events published to the broker.",
		}),
		OutboxBatchSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "outbox_batch_duration_seconds", Help: "Time to claim, publish and mark one outbox batch.",
			Buckets: prometheus.DefBuckets,
		}),
		ConsumerMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "consumer_messages_total", Help: "Consumed messages by result (ok, retry, dlq).",
		}, []string{"result"}),
	}
	reg.MustRegister(m.HTTPRequests, m.HTTPDuration, m.HTTPInFlight, m.CacheOps,
		m.OutboxPublished, m.OutboxBatchSeconds, m.ConsumerMessages)
	return m
}

// RegisterPool exposes pgx pool saturation. "acquire wait" growing while "acquired" sits at max
// is the classic sign the pool (or the database) is the bottleneck.
func (m *Metrics) RegisterPool(namespace string, pool *pgxpool.Pool) {
	gauge := func(name, help string, f func(*pgxpool.Stat) float64) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: namespace, Name: "db_pool_" + name, Help: help},
			func() float64 { return f(pool.Stat()) })
	}
	counter := func(name, help string, f func(*pgxpool.Stat) float64) prometheus.Collector {
		return prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: namespace, Name: "db_pool_" + name, Help: help},
			func() float64 { return f(pool.Stat()) })
	}
	m.Registry.MustRegister(
		gauge("acquired_conns", "Connections currently in use.", func(s *pgxpool.Stat) float64 { return float64(s.AcquiredConns()) }),
		gauge("idle_conns", "Idle connections.", func(s *pgxpool.Stat) float64 { return float64(s.IdleConns()) }),
		gauge("total_conns", "Open connections.", func(s *pgxpool.Stat) float64 { return float64(s.TotalConns()) }),
		gauge("max_conns", "Configured maximum connections.", func(s *pgxpool.Stat) float64 { return float64(s.MaxConns()) }),
		counter("acquire_total", "Cumulative connection acquires.", func(s *pgxpool.Stat) float64 { return float64(s.AcquireCount()) }),
		counter("empty_acquire_total", "Acquires that had to wait for a connection.", func(s *pgxpool.Stat) float64 { return float64(s.EmptyAcquireCount()) }),
		counter("acquire_wait_seconds_total", "Cumulative time spent waiting for a connection.", func(s *pgxpool.Stat) float64 { return s.AcquireDuration().Seconds() }),
	)
}

// Handler serves the registry in Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry})
}
