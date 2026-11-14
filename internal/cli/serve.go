package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/config"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/refresher"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/server"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/store"
)

// openStore connects to Postgres, optionally migrating and seeding, and
// refuses to continue on an outdated schema.
func openStore(ctx context.Context, cfg config.Config, log *slog.Logger, migrate, seed bool) (*pgxpool.Pool, *store.Store, error) {
	if err := cfg.RequireDB(); err != nil {
		return nil, nil, err
	}
	pool, err := store.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (*pgxpool.Pool, *store.Store, error) {
		pool.Close()
		return nil, nil, err
	}
	if migrate {
		if err := store.MigrateUp(ctx, pool, log); err != nil {
			return fail(err)
		}
	} else if err := store.EnsureSchemaCurrent(ctx, pool); err != nil {
		return fail(fmt.Errorf("%w (run `hnfetcher migrate up` or set DB_AUTO_MIGRATE=true)", err))
	}
	if seed {
		if err := store.SeedUp(ctx, pool, log); err != nil {
			return fail(err)
		}
	}
	return pool, store.New(pool), nil
}

func newRefresher(cfg config.Config, log *slog.Logger, st *store.Store) *refresher.Refresher {
	return refresher.New(newCrawler(cfg, log), st, &refresher.Cache{}, refresher.Options{
		Interval:         cfg.RefreshInterval,
		Enabled:          cfg.RefreshEnabled,
		StoriesRequested: cfg.TopStories,
		RetentionCount:   cfg.RetentionCount,
		RetentionAge:     cfg.RetentionAge,
	}, log)
}

// runServe runs the refresher and the HTTP API until ctx is canceled
// (SIGINT/SIGTERM), then shuts down in order: fail probes, stop accepting
// requests, stop the refresher, close the pool.
func runServe(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	pool, st, err := openStore(ctx, cfg, log, cfg.DBAutoMigrate, cfg.DBAutoSeed)
	if err != nil {
		return err
	}
	defer pool.Close()

	ref := newRefresher(cfg, log, st)
	srv := server.New(server.Deps{
		Store:           st,
		Cache:           ref.Cache(),
		Refresher:       ref,
		HN:              newHN(cfg),
		Version:         Version,
		RefreshInterval: cfg.RefreshInterval,
		Log:             log,
	})

	refCtx, stopRefresher := context.WithCancel(context.WithoutCancel(ctx))
	var wg sync.WaitGroup
	wg.Go(func() { ref.Run(refCtx) })

	listenErr := make(chan error, 1)
	go func() { listenErr <- srv.Listen(":" + strconv.Itoa(cfg.Port)) }()
	log.Info("hnfetcher started", "version", Version, "port", cfg.Port,
		"refresh_enabled", cfg.RefreshEnabled, "refresh_interval", cfg.RefreshInterval.String())

	var runErr error
	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case runErr = <-listenErr:
		runErr = fmt.Errorf("http server: %w", runErr)
	}

	srv.MarkShuttingDown()
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	stopRefresher()
	wg.Wait()
	log.Info("stopped")
	return runErr
}

// runCrawl performs one crawl and stores it, like a scheduled run.
func runCrawl(ctx context.Context, cfg config.Config, log *slog.Logger, args []string, stdout io.Writer) error {
	fs := newFlags("crawl")
	limit := fs.Int("limit", cfg.TopStories, "number of top stories (1-100)")
	if err := parse(fs, args); err != nil {
		return err
	}
	if *limit < 1 || *limit > 100 {
		return fmt.Errorf("%w: --limit must be between 1 and 100", ErrUsage)
	}
	cfg.TopStories = *limit
	pool, st, err := openStore(ctx, cfg, log, cfg.DBAutoMigrate, false)
	if err != nil {
		return err
	}
	defer pool.Close()

	snap, err := newRefresher(cfg, log, st).RunOnce(ctx, refresher.TriggerCLI)
	if errors.Is(err, refresher.ErrBusy) {
		return errors.New("another crawl is already running (crawl lock held); try again later")
	}
	if snap != nil {
		fmt.Fprintf(stdout, "snapshot %d %s in %dms: %d stories, %d items, %d authors, %d missing, %d failed\n",
			snap.ID, snap.Status, snap.DurationMS, len(snap.Stories), snap.Stats.ItemsFetched,
			snap.Stats.AuthorsFetched, snap.Stats.MissingItems, snap.Stats.FailedItems)
		if snap.Error != "" {
			fmt.Fprintf(stdout, "  ! %s\n", snap.Error)
		}
	}
	return err
}

func snapshotFromDB(ctx context.Context, cfg config.Config, id int64, limit int, fs *flag.FlagSet, format, out string, stdout io.Writer) error {
	if err := cfg.RequireDB(); err != nil {
		return err
	}
	pool, err := store.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()
	st := store.New(pool)
	if id == 0 {
		if id, err = st.LatestSnapshotID(ctx); errors.Is(err, store.ErrNotFound) {
			return errors.New("no completed snapshot in the database yet; run `hnfetcher crawl`")
		} else if err != nil {
			return err
		}
	}
	snap, err := st.Snapshot(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("snapshot %d not found", id)
	}
	if err != nil {
		return err
	}
	// --limit only trims a stored snapshot when given explicitly.
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "limit" {
			snap = snap.Limit(limit)
		}
	})
	return write(snap, format, out, stdout)
}
