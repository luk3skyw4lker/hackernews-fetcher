package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

//go:embed seeds/*.sql
var seedsFS embed.FS

// MigrationSet selects which goose provider to use.
type MigrationSet string

const (
	// Schema migrations, tracked in goose_db_version.
	Schema MigrationSet = "migrations"
	// Seeds (demo data), tracked separately in goose_seed_version.
	Seeds MigrationSet = "seeds"
)

func (s MigrationSet) table() string {
	if s == Seeds {
		return "goose_seed_version"
	}
	return "goose_db_version"
}

func (s MigrationSet) fsys() embed.FS {
	if s == Seeds {
		return seedsFS
	}
	return migrationsFS
}

// lockID gives each set its own Postgres advisory lock for goose.
func (s MigrationSet) lockID() int64 {
	if s == Seeds {
		return 0x484e5345 // "HNSE"
	}
	return 0x484e4d47 // "HNMG"
}

// Migrator wraps a goose Provider for one migration set.
type Migrator struct {
	Set      MigrationSet
	provider *goose.Provider
	closeDB  func() error
}

// NewMigrator builds a goose provider over the embedded SQL files, reusing the
// pgx pool via database/sql. A Postgres session lock makes concurrent
// startups (several replicas) safe.
func NewMigrator(pool *pgxpool.Pool, set MigrationSet, log *slog.Logger) (*Migrator, error) {
	sub, err := fs.Sub(set.fsys(), string(set))
	if err != nil {
		return nil, err
	}
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockID(set.lockID()))
	if err != nil {
		return nil, err
	}
	db := stdlib.OpenDBFromPool(pool)
	opts := []goose.ProviderOption{
		goose.WithTableName(set.table()),
		goose.WithSessionLocker(locker),
	}
	if log != nil {
		opts = append(opts, goose.WithSlog(log.With("goose", string(set))))
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, sub, opts...)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Migrator{Set: set, provider: p, closeDB: db.Close}, nil
}

// Close releases the database/sql handle (the pgx pool stays open).
func (m *Migrator) Close() error { return m.closeDB() }

// Up applies all pending files.
func (m *Migrator) Up(ctx context.Context) ([]*goose.MigrationResult, error) {
	return m.provider.Up(ctx)
}

// Down rolls back the latest applied file.
func (m *Migrator) Down(ctx context.Context) (*goose.MigrationResult, error) {
	return m.provider.Down(ctx)
}

// DownTo rolls back to (and keeping) version v; 0 rolls back everything.
func (m *Migrator) DownTo(ctx context.Context, v int64) ([]*goose.MigrationResult, error) {
	return m.provider.DownTo(ctx, v)
}

// Redo rolls back the latest file and applies it again.
func (m *Migrator) Redo(ctx context.Context) error {
	if _, err := m.provider.Down(ctx); err != nil {
		return err
	}
	_, err := m.provider.UpByOne(ctx)
	return err
}

// Status lists every file and whether it is applied.
func (m *Migrator) Status(ctx context.Context) ([]*goose.MigrationStatus, error) {
	return m.provider.Status(ctx)
}

// Versions returns the current DB version and the latest available version.
func (m *Migrator) Versions(ctx context.Context) (current, target int64, err error) {
	return m.provider.GetVersions(ctx)
}

// HasPending reports whether unapplied files exist.
func (m *Migrator) HasPending(ctx context.Context) (bool, error) { return m.provider.HasPending(ctx) }

// ErrSchemaNotCurrent is returned when seeding against an outdated schema.
var ErrSchemaNotCurrent = errors.New("schema has pending migrations; run `hnfetcher migrate up` first")

// EnsureSchemaCurrent fails if schema migrations are pending.
func EnsureSchemaCurrent(ctx context.Context, pool *pgxpool.Pool) error {
	m, err := NewMigrator(pool, Schema, nil)
	if err != nil {
		return err
	}
	defer m.Close()
	pending, err := m.HasPending(ctx)
	if err != nil {
		return fmt.Errorf("check schema version: %w", err)
	}
	if pending {
		return ErrSchemaNotCurrent
	}
	return nil
}

// MigrateUp applies all schema migrations.
func MigrateUp(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	return runUp(ctx, pool, Schema, log)
}

// SeedUp applies all seeds; the schema must be current.
func SeedUp(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	if err := EnsureSchemaCurrent(ctx, pool); err != nil {
		return err
	}
	return runUp(ctx, pool, Seeds, log)
}

func runUp(ctx context.Context, pool *pgxpool.Pool, set MigrationSet, log *slog.Logger) error {
	m, err := NewMigrator(pool, set, log)
	if err != nil {
		return err
	}
	defer m.Close()
	if _, err := m.Up(ctx); err != nil {
		return fmt.Errorf("%s up: %w", set, err)
	}
	return nil
}
