// Package bootstrap builds the infrastructure shared by every binary (api, worker):
// config, logging, tracing, metrics, PostgreSQL, Redis and health checks.
// Binaries add their own pieces (HTTP server, Kafka clients) on top of a Runtime.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/yourorg/go-clean-template/internal/platform/config"
	"github.com/yourorg/go-clean-template/internal/platform/database"
	"github.com/yourorg/go-clean-template/internal/platform/health"
	"github.com/yourorg/go-clean-template/internal/platform/httpserver"
	"github.com/yourorg/go-clean-template/internal/platform/logger"
	"github.com/yourorg/go-clean-template/internal/platform/metrics"
	"github.com/yourorg/go-clean-template/internal/platform/redisx"
	"github.com/yourorg/go-clean-template/internal/platform/telemetry"
)

type closer struct {
	name string
	fn   func(context.Context) error
}

// Runtime is the shared infrastructure of one process.
type Runtime struct {
	Cfg     *config.Config
	Log     *slog.Logger
	Metrics *metrics.Metrics
	Health  *health.Registry
	Pool    *pgxpool.Pool
	Redis   *redis.Client

	closers []closer
}

// SignalContext is cancelled on SIGINT or SIGTERM (what Kubernetes sends on pod termination).
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// New initialises shared infrastructure. service is the binary name ("api", "worker").
func New(ctx context.Context, service string) (*Runtime, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	name := cfg.ServiceName + "-" + service
	log := logger.New(cfg.LogLevel, cfg.Env, name)
	slog.SetDefault(log)

	rt := &Runtime{Cfg: cfg, Log: log, Health: &health.Registry{}}

	// Set GOMEMLIMIT to 90% of the container memory limit so the GC works harder before the
	// kernel OOM-kills the pod. (GOMAXPROCS is already container-aware since Go 1.25.)
	// Outside a cgroup (a laptop) there is no limit to read, which is fine.
	if limit, err := memlimit.Set(memlimit.WithRatio(0.9), memlimit.WithProvider(memlimit.FromCgroup), memlimit.WithLogger(slog.New(slog.DiscardHandler))); err != nil {
		log.Debug("GOMEMLIMIT not set", slog.Any("reason", err))
	} else {
		log.Info("GOMEMLIMIT set from cgroup", slog.Int64("bytes", limit))
	}

	shutdownTracing, err := telemetry.Setup(ctx, cfg.Otel, name, cfg.Env)
	if err != nil {
		return nil, err
	}
	rt.AddCloser("telemetry", shutdownTracing)

	rt.Metrics = metrics.New("app")

	pool, err := database.NewPool(ctx, cfg.DB, name)
	if err != nil {
		_ = rt.Close(context.Background())
		return nil, err
	}
	rt.Pool = pool
	rt.AddCloser("postgres", func(context.Context) error { pool.Close(); return nil })
	rt.Metrics.RegisterPool("app", pool)
	rt.Health.Add("postgres", true, pool.Ping)

	rdb, err := redisx.New(ctx, cfg.Redis)
	if err != nil {
		_ = rt.Close(context.Background())
		return nil, err
	}
	rt.Redis = rdb
	rt.AddCloser("redis", func(context.Context) error { return rdb.Close() })
	// Not critical: cache, rate limiting and idempotency fail open, see docs/ARCHITECTURE.md.
	rt.Health.Add("redis", false, func(ctx context.Context) error { return rdb.Ping(ctx).Err() })

	return rt, nil
}

// AddCloser registers a function run (in reverse order) by Close.
func (r *Runtime) AddCloser(name string, fn func(context.Context) error) {
	r.closers = append(r.closers, closer{name: name, fn: fn})
}

// Close releases resources in reverse order of creation.
func (r *Runtime) Close(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var errs []error
	for i := len(r.closers) - 1; i >= 0; i-- {
		c := r.closers[i]
		if err := c.fn(ctx); err != nil {
			errs = append(errs, fmt.Errorf("close %s: %w", c.name, err))
		}
	}
	return errors.Join(errs...)
}

// StartOps serves the internal ops endpoints (/healthz, /readyz, /metrics) and, when configured,
// pprof on a loopback address. The returned function stops them; call it LAST during shutdown so
// /readyz keeps answering (503) while the main work drains.
func (r *Runtime) StartOps() (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	serve := func(srv *http.Server) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := httpserver.Run(ctx, srv, r.Log, nil, 0, 5*time.Second); err != nil {
				r.Log.Error("ops server failed", slog.String("addr", srv.Addr), slog.Any("error", err))
			}
		}()
	}

	ops := httpserver.OpsHandler(r.Health.Live(), r.Health.Ready(), r.Metrics.Handler())
	serve(httpserver.New(r.Cfg.Ops.Addr, ops, r.Cfg.HTTP))
	if r.Cfg.Ops.PprofAddr != "" {
		// pprof needs long responses (30s CPU profiles), so no WriteTimeout here.
		serve(&http.Server{Addr: r.Cfg.Ops.PprofAddr, Handler: httpserver.PprofHandler(), ReadHeaderTimeout: 5 * time.Second})
	}
	return func() { cancel(); wg.Wait() }
}
