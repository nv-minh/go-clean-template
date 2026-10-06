// Package repository implements the persistence ports of package domain with pgx and sqlc.
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yourorg/go-clean-template/internal/adapter/repository/sqlcdb"
	"github.com/yourorg/go-clean-template/internal/domain"
)

type txKey struct{}

// Store owns the pool and hands out sqlc queries bound to the ambient transaction, if any.
// It also implements domain.TxManager.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// queries returns Queries bound to the transaction stored in ctx, or to the pool otherwise.
func (s *Store) queries(ctx context.Context) *sqlcdb.Queries {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return sqlcdb.New(tx)
	}
	return sqlcdb.New(s.pool)
}

// WithinTx runs fn in a transaction. A nested call joins the outer transaction.
// Keep transactions short: they pin a pooled connection for their whole duration.
func (s *Store) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return fn(ctx)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		rollback(tx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// rollback uses a detached context so a cancelled request still releases its connection cleanly.
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// mapErr translates driver errors into domain errors.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
		return fmt.Errorf("%w: %s", domain.ErrConflict, pgErr.ConstraintName)
	}
	return err
}
