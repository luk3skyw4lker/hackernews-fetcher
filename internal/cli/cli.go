// Package cli implements the hnfetcher subcommands.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/config"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/crawler"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/hn"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/store"
)

// Version is set at build time via -ldflags "-X .../internal/cli.Version=...".
var Version = "dev"

// ErrUsage signals a usage error (exit code 2).
var ErrUsage = errors.New("usage error")

const usage = `hnfetcher %s: HackerNews top stories with full comment trees and authors.

Usage:
  hnfetcher <command> [flags]

Commands:
  serve                          run migrations, background refresher and HTTP API (default)
  crawl     [--limit N]          crawl once and store the snapshot in Postgres
  snapshot  [--limit N] [--format json|text] [--out FILE]
                                 crawl once and print the result (no database)
  snapshot  --from-db [--id N]   print a stored snapshot
  migrate   up|down|down-to N|redo|status|version
  seed      up|down|reset|status
  healthcheck                    probe http://127.0.0.1:$PORT/livez (exit 0/1)
  version                        print the version

Configuration comes from environment variables (see README); flags override them.
`

// Run executes the CLI and returns an error to be reported by main.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	cmd := "serve"
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}
	switch cmd {
	case "serve":
		if err := parse(newFlags("serve"), args); err != nil {
			return err
		}
		return runServe(ctx, cfg, log)
	case "crawl":
		return runCrawl(ctx, cfg, log, args, stdout)
	case "snapshot":
		return runSnapshot(ctx, cfg, log, args, stdout)
	case "migrate":
		return runMigrations(ctx, cfg, log, store.Schema, args, stdout)
	case "seed":
		return runMigrations(ctx, cfg, log, store.Seeds, args, stdout)
	case "healthcheck":
		return runHealthcheck(ctx, cfg)
	case "version", "--version", "-v":
		fmt.Fprintln(stdout, Version)
		return nil
	case "help", "-h", "--help":
		fmt.Fprintf(stdout, usage, Version)
		return nil
	default:
		fmt.Fprintf(stderr, usage, Version)
		return fmt.Errorf("%w: unknown command %q", ErrUsage, cmd)
	}
}

func newFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard) // errors are returned and printed by main
	return fs
}

func parse(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrUsage, fs.Name(), err)
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("%w: %s: unexpected argument %q", ErrUsage, fs.Name(), fs.Arg(0))
	}
	return nil
}

func newHN(cfg config.Config) *hn.Client {
	return hn.New(hn.Options{
		BaseURL:        cfg.HNBaseURL,
		MaxConcurrency: cfg.HNMaxConcurrency,
		RequestTimeout: cfg.HNRequestTimeout,
		MaxRetries:     cfg.HNMaxRetries,
	})
}

func newCrawler(cfg config.Config, log *slog.Logger) *crawler.Crawler {
	return crawler.New(newHN(cfg), crawler.Options{
		TopStories:  cfg.TopStories,
		IncludeJobs: cfg.IncludeJobs,
		MaxDepth:    cfg.MaxDepth,
		Timeout:     cfg.CrawlTimeout,
	}, log)
}

func runSnapshot(ctx context.Context, cfg config.Config, log *slog.Logger, args []string, stdout io.Writer) error {
	fs := newFlags("snapshot")
	limit := fs.Int("limit", cfg.TopStories, "number of top stories (1-100)")
	format := fs.String("format", "json", "output format: json or text")
	out := fs.String("out", "", "write to FILE instead of stdout")
	fromDB := fs.Bool("from-db", false, "print a stored snapshot instead of crawling")
	id := fs.Int64("id", 0, "snapshot id with --from-db (default: latest)")
	if err := parse(fs, args); err != nil {
		return err
	}
	if *limit < 1 || *limit > 100 {
		return fmt.Errorf("%w: --limit must be between 1 and 100", ErrUsage)
	}
	if *format != "json" && *format != "text" {
		return fmt.Errorf("%w: --format must be json or text", ErrUsage)
	}
	if *fromDB || *id != 0 {
		return snapshotFromDB(ctx, cfg, *id, *limit, fs, *format, *out, stdout)
	}

	cfg.TopStories = *limit
	snap, err := newCrawler(cfg, log).Crawl(ctx, "cli")
	if snap == nil {
		return err
	}
	if werr := write(snap, *format, *out, stdout); werr != nil {
		return werr
	}
	return err // aborted crawl: output written, but exit non-zero
}

func write(snap *model.Snapshot, format, out string, stdout io.Writer) (err error) {
	w := stdout
	if out != "" {
		f, ferr := os.Create(out)
		if ferr != nil {
			return ferr
		}
		defer func() {
			if cerr := f.Close(); err == nil {
				err = cerr
			}
		}()
		w = f
	}
	if format == "text" {
		return model.RenderText(w, snap)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(snap)
}

func runMigrations(ctx context.Context, cfg config.Config, log *slog.Logger, set store.MigrationSet, args []string, stdout io.Writer) error {
	name := "migrate"
	if set == store.Seeds {
		name = "seed"
	}
	if len(args) == 0 {
		return fmt.Errorf("%w: %s needs a subcommand", ErrUsage, name)
	}
	if err := cfg.RequireDB(); err != nil {
		return err
	}
	pool, err := store.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()
	if set == store.Seeds && (args[0] == "up" || args[0] == "reset") {
		if err := store.EnsureSchemaCurrent(ctx, pool); err != nil {
			return err
		}
	}
	m, err := store.NewMigrator(pool, set, log)
	if err != nil {
		return err
	}
	defer m.Close()

	sub, rest := args[0], args[1:]
	switch {
	case sub == "up" && len(rest) == 0:
		_, err = m.Up(ctx)
	case sub == "down" && len(rest) == 0:
		_, err = m.Down(ctx)
	case sub == "down-to" && set == store.Schema && len(rest) == 1:
		v, perr := strconv.ParseInt(rest[0], 10, 64)
		if perr != nil {
			return fmt.Errorf("%w: down-to needs a version number", ErrUsage)
		}
		_, err = m.DownTo(ctx, v)
	case sub == "redo" && set == store.Schema && len(rest) == 0:
		err = m.Redo(ctx)
	case sub == "reset" && set == store.Seeds && len(rest) == 0:
		if _, err = m.DownTo(ctx, 0); err == nil {
			_, err = m.Up(ctx)
		}
	case sub == "status" && len(rest) == 0:
		return printStatus(ctx, m, stdout)
	case sub == "version" && len(rest) == 0:
		cur, target, verr := m.Versions(ctx)
		if verr != nil {
			return verr
		}
		fmt.Fprintf(stdout, "current: %d\nlatest:  %d\n", cur, target)
		return nil
	default:
		return fmt.Errorf("%w: unknown %s subcommand %q", ErrUsage, name, args)
	}
	if err != nil {
		return fmt.Errorf("%s %s: %w", name, sub, err)
	}
	return printStatus(ctx, m, stdout)
}

func printStatus(ctx context.Context, m *store.Migrator, w io.Writer) error {
	st, err := m.Status(ctx)
	if err != nil {
		return err
	}
	for _, s := range st {
		applied := "pending"
		if !s.AppliedAt.IsZero() {
			applied = s.AppliedAt.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(w, "%-8s %-28s %s\n", s.State, s.Source.Path, applied)
	}
	return nil
}

func runHealthcheck(ctx context.Context, cfg config.Config) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/livez", cfg.Port), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: status %d", resp.StatusCode)
	}
	return nil
}
