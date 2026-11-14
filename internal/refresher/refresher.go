// Package refresher runs crawls in the background and keeps the latest
// snapshot cached in memory.
//
// Every crawl, scheduled, manual or from the CLI, runs under a Postgres
// advisory lock, so only one crawl happens at a time across all replicas.
// A replica that loses the race (or runs with REFRESH_ENABLED=false) just
// reloads the latest snapshot from the database on each tick.
package refresher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/store"
)

// Trigger values stored in snapshots.triggered_by.
const (
	TriggerSchedule = "schedule"
	TriggerManual   = "manual"
	TriggerCLI      = "cli"
)

var (
	// ErrBusy means a crawl is already running (in this process or another).
	ErrBusy = errors.New("a crawl is already running")
	// ErrDisabled means this instance does not crawl (REFRESH_ENABLED=false).
	ErrDisabled = errors.New("refresh is disabled on this instance")
)

// Crawler produces snapshots.
type Crawler interface {
	Crawl(ctx context.Context, trigger string) (*model.Snapshot, error)
}

// Store is the persistence the refresher needs.
type Store interface {
	TryCrawlLock(ctx context.Context) (unlock func(), ok bool, err error)
	CreateRunning(ctx context.Context, trigger string, storiesRequested int) (int64, error)
	SaveSnapshot(ctx context.Context, id int64, snap *model.Snapshot) error
	MarkFailed(ctx context.Context, id int64, reason string) error
	FailStaleRunning(ctx context.Context) (int64, error)
	ApplyRetention(ctx context.Context, keep int, maxAge time.Duration) (int64, error)
	LatestSnapshotID(ctx context.Context) (int64, error)
	Snapshot(ctx context.Context, id int64) (*model.Snapshot, error)
}

// Options configures the refresher.
type Options struct {
	Interval         time.Duration
	Enabled          bool
	StoriesRequested int
	RetentionCount   int
	RetentionAge     time.Duration
	SaveTimeout      time.Duration // bound for writes that outlive a canceled crawl (default 30s)
}

// State is what /healthz reports about the refresher.
type State struct {
	Enabled             bool       `json:"enabled"`
	Running             bool       `json:"running"`
	LastStartedAt       *time.Time `json:"last_started_at,omitempty"`
	LastFinishedAt      *time.Time `json:"last_finished_at,omitempty"`
	LastStatus          string     `json:"last_status,omitempty"`
	LastError           string     `json:"last_error,omitempty"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
}

// Refresher schedules crawls and owns the latest-snapshot cache.
type Refresher struct {
	crawler Crawler
	store   Store
	cache   *Cache
	opts    Options
	log     *slog.Logger

	trigger chan struct{}
	running atomic.Bool

	mu    sync.Mutex
	state State
}

// New builds a refresher. cache may be shared with the HTTP server.
func New(c Crawler, s Store, cache *Cache, opts Options, log *slog.Logger) *Refresher {
	if log == nil {
		log = slog.Default()
	}
	if opts.SaveTimeout <= 0 {
		opts.SaveTimeout = 30 * time.Second
	}
	if cache == nil {
		cache = &Cache{}
	}
	return &Refresher{
		crawler: c, store: s, cache: cache, opts: opts, log: log.With("component", "refresher"),
		trigger: make(chan struct{}, 1),
		state:   State{Enabled: opts.Enabled},
	}
}

// Cache returns the latest-snapshot cache.
func (r *Refresher) Cache() *Cache { return r.cache }

// State returns a copy of the current state.
func (r *Refresher) State() State {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.state
	st.Running = r.running.Load()
	return st
}

// Trigger asks for a crawl as soon as possible. Repeated calls before the
// loop picks it up collapse into one run.
func (r *Refresher) Trigger() error {
	if !r.opts.Enabled {
		return ErrDisabled
	}
	if r.running.Load() {
		return ErrBusy
	}
	select {
	case r.trigger <- struct{}{}:
	default: // one already queued
	}
	return nil
}

// Run loads the latest stored snapshot, crawls immediately (if enabled), then
// crawls on every tick or manual trigger until ctx is done. A tick that fires
// during a long crawl is coalesced by time.Ticker, so runs never stack up.
func (r *Refresher) Run(ctx context.Context) {
	if err := r.Sync(ctx); err != nil && !errors.Is(err, store.ErrNotFound) {
		r.log.Warn("load latest snapshot", "err", err)
	}
	if r.opts.Enabled {
		r.cleanupStale(ctx)
		r.runScheduled(ctx, TriggerSchedule)
	}
	t := time.NewTicker(r.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if r.opts.Enabled {
				r.runScheduled(ctx, TriggerSchedule)
			} else if err := r.Sync(ctx); err != nil && !errors.Is(err, store.ErrNotFound) {
				r.log.Warn("sync latest snapshot", "err", err)
			}
		case <-r.trigger:
			r.runScheduled(ctx, TriggerManual)
		}
	}
}

func (r *Refresher) runScheduled(ctx context.Context, trigger string) {
	_, err := r.RunOnce(ctx, trigger)
	switch {
	case errors.Is(err, ErrBusy):
		// Another replica is crawling; pick up whatever it last stored.
		r.log.Info("crawl skipped: another instance holds the crawl lock")
		if err := r.Sync(ctx); err != nil && !errors.Is(err, store.ErrNotFound) {
			r.log.Warn("sync latest snapshot", "err", err)
		}
	case err != nil && ctx.Err() == nil:
		r.log.Error("crawl failed", "trigger", trigger, "err", err)
	}
}

// cleanupStale marks rows left "running" by a dead process as failed. It
// only does so while holding the crawl lock, so it can't touch a crawl that
// another replica is running right now.
func (r *Refresher) cleanupStale(ctx context.Context) {
	unlock, ok, err := r.store.TryCrawlLock(ctx)
	if err != nil || !ok {
		return
	}
	defer unlock()
	if n, err := r.store.FailStaleRunning(ctx); err != nil {
		r.log.Warn("fail stale running snapshots", "err", err)
	} else if n > 0 {
		r.log.Info("marked interrupted snapshots as failed", "count", n)
	}
}

// Sync loads the latest stored snapshot into the cache if it is newer than
// the cached one.
func (r *Refresher) Sync(ctx context.Context) error {
	id, err := r.store.LatestSnapshotID(ctx)
	if err != nil {
		return err
	}
	if cur := r.cache.Get(); cur != nil && cur.Snapshot.ID >= id {
		return nil
	}
	snap, err := r.store.Snapshot(ctx, id)
	if err != nil {
		return err
	}
	return r.cache.Set(snap)
}

// RunOnce performs one crawl under the advisory lock and stores the result.
// It returns ErrBusy if a crawl is already running anywhere. The returned
// snapshot is the stored one (nil if nothing was stored); a non-nil error
// with a non-nil snapshot means the crawl was aborted but its partial data
// was saved.
func (r *Refresher) RunOnce(ctx context.Context, trigger string) (*model.Snapshot, error) {
	if !r.running.CompareAndSwap(false, true) {
		return nil, ErrBusy
	}
	defer r.running.Store(false)

	unlock, ok, err := r.store.TryCrawlLock(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrBusy
	}
	defer unlock()

	started := time.Now().UTC()
	r.mu.Lock()
	r.state.LastStartedAt = &started
	r.mu.Unlock()

	snap, err := r.crawlAndSave(ctx, trigger)
	r.record(snap, err)
	return snap, err
}

func (r *Refresher) crawlAndSave(ctx context.Context, trigger string) (*model.Snapshot, error) {
	id, err := r.store.CreateRunning(ctx, trigger, r.opts.StoriesRequested)
	if err != nil {
		return nil, err
	}
	log := r.log.With("snapshot_id", id, "trigger", trigger)
	log.Info("crawl started")

	snap, crawlErr := r.crawler.Crawl(ctx, trigger)

	// The crawl may have been canceled (shutdown), but whatever it produced
	// should still be recorded, so writes use a context that outlives ctx.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.opts.SaveTimeout)
	defer cancel()

	if snap == nil || snap.Status == model.SnapshotFailed {
		reason := "crawl produced no data"
		switch {
		case crawlErr != nil:
			reason = crawlErr.Error()
		case snap != nil && snap.Error != "":
			reason = snap.Error
		}
		if err := r.store.MarkFailed(wctx, id, reason); err != nil {
			log.Error("mark snapshot failed", "err", err)
		}
		if crawlErr == nil {
			crawlErr = errors.New(reason)
		}
		return nil, crawlErr
	}

	if err := r.store.SaveSnapshot(wctx, id, snap); err != nil {
		if merr := r.store.MarkFailed(wctx, id, "save: "+err.Error()); merr != nil {
			log.Error("mark snapshot failed", "err", merr)
		}
		return nil, fmt.Errorf("save snapshot %d: %w", id, err)
	}
	if err := r.cache.Set(snap); err != nil {
		log.Error("cache snapshot", "err", err)
	}
	log.Info("crawl finished", "status", snap.Status, "duration_ms", snap.DurationMS,
		"items", snap.Stats.ItemsFetched, "authors", snap.Stats.AuthorsFetched,
		"missing", snap.Stats.MissingItems, "failed", snap.Stats.FailedItems)

	if r.opts.RetentionCount > 0 || r.opts.RetentionAge > 0 {
		if n, err := r.store.ApplyRetention(wctx, r.opts.RetentionCount, r.opts.RetentionAge); err != nil {
			log.Warn("retention", "err", err)
		} else if n > 0 {
			log.Info("retention deleted old snapshots", "count", n)
		}
	}
	return snap, crawlErr
}

func (r *Refresher) record(snap *model.Snapshot, err error) {
	now := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.LastFinishedAt = &now
	r.state.LastError = ""
	if err != nil {
		r.state.LastError = err.Error()
	}
	if snap == nil {
		r.state.LastStatus = string(model.SnapshotFailed)
		r.state.ConsecutiveFailures++
		return
	}
	r.state.LastStatus = string(snap.Status)
	if snap.Error != "" && r.state.LastError == "" {
		r.state.LastError = snap.Error
	}
	r.state.ConsecutiveFailures = 0
}
