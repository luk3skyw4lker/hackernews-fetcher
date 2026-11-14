// Package store persists snapshots in PostgreSQL using a schema that mirrors
// the API output, and loads them back.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a snapshot does not exist.
var ErrNotFound = errors.New("not found")

// Store reads and writes snapshots.
type Store struct {
	pool *pgxpool.Pool
}

// New wraps an existing pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Pool exposes the underlying pool.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Ping checks connectivity.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Connect opens a pgx pool and verifies connectivity, retrying briefly so the
// service tolerates Postgres starting a few seconds after it.
func Connect(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	var pingErr error
	for attempt := range 10 {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		pingErr = pool.Ping(pctx)
		cancel()
		if pingErr == nil {
			return pool, nil
		}
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 300 * time.Millisecond):
		}
	}
	pool.Close()
	return nil, fmt.Errorf("connect to postgres: %w", pingErr)
}
