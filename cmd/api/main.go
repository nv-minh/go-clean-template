// Command api serves the public REST API. It is stateless: scale it horizontally behind a load balancer.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"golang.org/x/sync/errgroup"

	"github.com/yourorg/go-clean-template/internal/adapter/httpapi"
	"github.com/yourorg/go-clean-template/internal/adapter/redisstore"
	"github.com/yourorg/go-clean-template/internal/adapter/repository"
	"github.com/yourorg/go-clean-template/internal/platform/bootstrap"
	"github.com/yourorg/go-clean-template/internal/platform/httpserver"
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

// run is the composition root: the only place that knows every concrete type.
func run() error {
	ctx, stop := bootstrap.SignalContext()
	defer stop()

	rt, err := bootstrap.New(ctx, "api")
	if err != nil {
		return err
	}
	defer func() { _ = rt.Close(context.Background()) }()
	cfg, log := rt.Cfg, rt.Log
	log.Info("starting api", slog.String("version", version))

	// Adapters (outer layer).
	store := repository.NewStore(rt.Pool)
	orderRepo := repository.NewOrderRepository(store)
	outbox := repository.NewOutbox(store)
	cache := redisstore.NewOrderCache(rt.Redis, cfg.Redis.OrderCacheTTL, rt.Metrics)
	realtime := redisstore.NewRealtime(rt.Redis, log)

	// Use cases (inner layer) receive adapters through their ports.
	orders := usecase.NewOrderService(orderRepo, outbox, store, cache, log)

	deps := httpapi.Deps{
		Orders:      orders,
		Subscriber:  realtime,
		Idempotency: redisstore.NewIdempotency(rt.Redis, cfg.HTTP.IdempotencyTTL),
		Log:         log,
		Metrics:     rt.Metrics,
		Options: httpapi.Options{
			RequestTimeout:    cfg.HTTP.RequestTimeout,
			MaxBodyBytes:      cfg.HTTP.MaxBodyBytes,
			CORSOrigins:       cfg.HTTP.CORSOrigins,
			TrustProxyHeaders: cfg.HTTP.TrustProxyHeaders,
			SSEHeartbeat:      cfg.HTTP.SSEHeartbeat,
			SSEMaxConnection:  cfg.HTTP.SSEMaxConnection,
		},
	}
	if cfg.RateLimit.Enabled {
		deps.Limiter = redisstore.NewRateLimiter(rt.Redis, cfg.RateLimit.RPS, cfg.RateLimit.Burst)
	}

	stopOps := rt.StartOps()
	defer stopOps() // last to stop, so /readyz reports 503 while the API drains

	srv := httpserver.New(cfg.HTTP.Addr, httpapi.NewRouter(deps), cfg.HTTP)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return realtime.Run(gctx) })
	g.Go(func() error {
		return httpserver.Run(gctx, srv, log, rt.Health.MarkShuttingDown, cfg.HTTP.ShutdownDelay, cfg.HTTP.ShutdownTimeout)
	})
	return g.Wait()
}
