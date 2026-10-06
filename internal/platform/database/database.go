// Package database creates the PostgreSQL connection pool.
package database

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yourorg/go-clean-template/internal/platform/config"
)

// NewPool returns a tuned pgx pool.
//
// Sizing rule of thumb: total connections across ALL replicas must stay below the database
// max_connections (leave headroom for migrations/admin). Put PgBouncer (transaction pooling)
// in front when replicas * DB_MAX_CONNS gets large. pgx caches prepared statements per
// connection by default, which is incompatible with PgBouncer transaction mode unless you
// switch to QueryExecModeCacheDescribe (see docs/SCALING.md).
func NewPool(ctx context.Context, cfg config.DB, appName string) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pc.MaxConns = cfg.MaxConns
	pc.MinConns = cfg.MinConns
	pc.MaxConnLifetime = cfg.MaxConnLifetime
	pc.MaxConnLifetimeJitter = cfg.MaxConnLifetimeJitter
	pc.MaxConnIdleTime = cfg.MaxConnIdleTime
	pc.HealthCheckPeriod = cfg.HealthCheckPeriod
	pc.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	// Server-side guard rails: a runaway query can never hold a pooled connection forever.
	pc.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(cfg.StatementTimeout.Milliseconds(), 10)
	pc.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "30000"
	pc.ConnConfig.RuntimeParams["application_name"] = appName

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}
