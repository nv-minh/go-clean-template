//go:build integration

// Package integration exercises the real adapters against real PostgreSQL, Redis and Kafka started
// with testcontainers. Run with: make test-integration (Docker required).
package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yourorg/go-clean-template/db"
)

var (
	pool    *pgxpool.Pool
	rdb     *redis.Client
	brokers []string
)

func TestMain(m *testing.M) {
	code, err := run(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration setup:", err)
		code = 1
	}
	os.Exit(code)
}

func run(m *testing.M) (int, error) {
	ctx := context.Background()

	pg, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("app"), postgres.WithUsername("app"), postgres.WithPassword("app"),
		postgres.BasicWaitStrategies())
	if err != nil {
		return 0, fmt.Errorf("start postgres: %w", err)
	}
	defer func() { _ = testcontainers.TerminateContainer(pg) }()
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return 0, err
	}
	src, err := iofs.New(db.Migrations, "migrations")
	if err != nil {
		return 0, err
	}
	mg, err := migrate.NewWithSourceInstance("iofs", src, strings.Replace(dsn, "postgres://", "pgx5://", 1))
	if err != nil {
		return 0, fmt.Errorf("init migrate: %w", err)
	}
	if err := mg.Up(); err != nil {
		return 0, fmt.Errorf("migrate up: %w", err)
	}
	if pool, err = pgxpool.New(ctx, dsn); err != nil {
		return 0, err
	}
	defer pool.Close()

	rc, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		return 0, fmt.Errorf("start redis: %w", err)
	}
	defer func() { _ = testcontainers.TerminateContainer(rc) }()
	uri, err := rc.ConnectionString(ctx)
	if err != nil {
		return 0, err
	}
	ropts, err := redis.ParseURL(uri)
	if err != nil {
		return 0, err
	}
	rdb = redis.NewClient(ropts)
	defer rdb.Close()

	kc, err := kafka.Run(ctx, "confluentinc/confluent-local:7.5.0", kafka.WithClusterID("it-cluster"),
		testcontainers.WithWaitStrategy(wait.ForLog("Kafka Server started").WithStartupTimeout(2*time.Minute)))
	if err != nil {
		return 0, fmt.Errorf("start kafka: %w", err)
	}
	defer func() { _ = testcontainers.TerminateContainer(kc) }()
	if brokers, err = kc.Brokers(ctx); err != nil {
		return 0, err
	}

	return m.Run(), nil
}

// createTopics makes explicit topics so tests control partition counts.
func createTopics(t *testing.T, partitions int32, topics ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).CreateTopics(t.Context(), partitions, 1, nil, topics...); err != nil {
		t.Fatal(err)
	}
}
