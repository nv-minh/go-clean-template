// Package httpserver creates hardened http.Server instances and runs them with graceful shutdown.
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/yourorg/go-clean-template/internal/platform/config"
)

// New returns a server with every timeout set. Missing timeouts are the number one cause of
// goroutine and file descriptor exhaustion (slowloris) in Go services.
func New(addr string, h http.Handler, cfg config.HTTP) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		MaxHeaderBytes:    1 << 16,
		BaseContext:       func(net.Listener) context.Context { return context.Background() },
	}
}

// Run serves until ctx is cancelled, then shuts down gracefully:
//  1. beforeShutdown is called (typically flips /readyz to 503),
//  2. it waits delay so the load balancer stops sending new requests,
//  3. in-flight requests get up to timeout to finish.
func Run(ctx context.Context, srv *http.Server, log *slog.Logger, beforeShutdown func(), delay, timeout time.Duration) error {
	errCh := make(chan error, 1)
	go func() {
		log.Info("http server listening", slog.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	if beforeShutdown != nil {
		beforeShutdown()
	}
	log.Info("shutdown signal received, draining", slog.Duration("delay", delay))
	time.Sleep(delay)

	shutCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		_ = srv.Close()
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	log.Info("http server stopped", slog.String("addr", srv.Addr))
	return nil
}

// OpsHandler builds the internal mux: probes, Prometheus metrics. Keep this port off the public
// ingress. pprof is served separately by PprofHandler on a loopback address.
func OpsHandler(live, ready http.HandlerFunc, metrics http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", live)
	mux.HandleFunc("GET /readyz", ready)
	mux.Handle("GET /metrics", metrics)
	return mux
}

// PprofHandler exposes runtime profiling endpoints (CPU, heap, goroutine, trace).
// Reach it with `kubectl port-forward` and `go tool pprof`.
func PprofHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
