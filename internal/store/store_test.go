package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/crawler"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/hn"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/hntest"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/store"
)

// adminURL points at the container's maintenance database; each test gets a
// fresh database created from it.
var adminURL string

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	if os.Getenv("TEST_DATABASE_URL") != "" { // e.g. CI with a service container
		adminURL = os.Getenv("TEST_DATABASE_URL")
		return m.Run()
	}
	image := os.Getenv("TEST_PG_IMAGE")
	if image == "" {
		image = "postgres:17-alpine"
	}
	// Bound startup so a stalled image pull fails fast instead of hanging.
	startCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ctx := context.Background()
	var ctr *postgres.PostgresContainer
	var err error
	func() {
		defer func() { // testcontainers panics when no Docker daemon is found
			if p := recover(); p != nil {
				err = fmt.Errorf("%v", p)
			}
		}()
		ctr, err = postgres.Run(startCtx, image,
			postgres.WithDatabase("hn"), postgres.WithUsername("hn"), postgres.WithPassword("hn"),
			postgres.BasicWaitStrategies())
	}()
	if err != nil {
		fmt.Fprintf(os.Stderr, "SKIP store tests: cannot start Postgres container (is Docker running?): %v\n", err)
		return 0
	}
	defer testcontainers.TerminateContainer(ctr) //nolint:errcheck
	adminURL, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return m.Run()
}

var dbSeq atomic.Int64

// freshDB creates an empty database for one test and returns a pool on it.
func freshDB(t *testing.T, migrate bool) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("t_%d_%d", os.Getpid(), dbSeq.Add(1))
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	cfg, _ := pgxpool.ParseConfig(adminURL)
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		admin.Close()
	})
	if migrate {
		if err := store.MigrateUp(ctx, pool, nil); err != nil {
			t.Fatalf("migrate up: %v", err)
		}
	}
	return pool
}

func crawlFixture(t *testing.T, top int) *model.Snapshot {
	t.Helper()
	srv := hntest.New(t)
	client := hn.New(hn.Options{BaseURL: srv.URL, MaxConcurrency: 8, RequestTimeout: time.Second, MaxRetries: 1, BackoffBase: time.Millisecond})
	snap, err := crawler.New(client, crawler.Options{TopStories: top}, nil).Crawl(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func save(t *testing.T, s *store.Store, snap *model.Snapshot) int64 {
	t.Helper()
	ctx := context.Background()
	id, err := s.CreateRunning(ctx, snap.Trigger, snap.StoriesRequested)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveSnapshot(ctx, id, snap); err != nil {
		t.Fatalf("save: %v", err)
	}
	return id
}

// The key test: what we store is exactly what the API would have served.
func TestRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := store.New(freshDB(t, true))
	snap := crawlFixture(t, 3)
	id := save(t, s, snap)

	got, err := s.Snapshot(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(snap, got, cmpopts.EquateEmpty()); diff != "" {
		t.Fatalf("round trip mismatch (-crawled +loaded):\n%s", diff)
	}
	latest, err := s.LatestSnapshot(ctx)
	if err != nil || latest.ID != id {
		t.Fatalf("latest = %v, %v", latest, err)
	}
}

func TestRunningSnapshotIsInvisible(t *testing.T) {
	ctx := context.Background()
	s := store.New(freshDB(t, true))
	if _, err := s.CreateRunning(ctx, "test", 5); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LatestSnapshot(ctx); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	n, err := s.FailStaleRunning(ctx)
	if err != nil || n != 1 {
		t.Fatalf("FailStaleRunning = %d, %v", n, err)
	}
	list, _ := s.ListSnapshots(ctx, store.ListOptions{Status: model.SnapshotFailed})
	if len(list) != 1 || list[0].Error == "" {
		t.Fatalf("failed list = %+v", list)
	}
}

func TestFailedSaveLeavesNoRows(t *testing.T) {
	ctx := context.Background()
	pool := freshDB(t, true)
	s := store.New(pool)
	snap := crawlFixture(t, 1)
	snap.Authors["bad"] = &model.Author{ID: "bad", Status: "bogus"} // violates CHECK
	id, _ := s.CreateRunning(ctx, "test", 1)
	if err := s.SaveSnapshot(ctx, id, snap); err == nil {
		t.Fatal("expected error")
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM items WHERE snapshot_id = $1`, id).Scan(&n)
	if n != 0 {
		t.Fatalf("%d item rows left after failed save", n)
	}
}

func TestListSnapshotsPagination(t *testing.T) {
	ctx := context.Background()
	s := store.New(freshDB(t, true))
	snap := crawlFixture(t, 1)
	var ids []int64
	for range 3 {
		cp := *snap
		ids = append(ids, save(t, s, &cp))
	}
	page, err := s.ListSnapshots(ctx, store.ListOptions{Limit: 2})
	if err != nil || len(page) != 2 || page[0].ID != ids[2] {
		t.Fatalf("page 1 = %+v, %v", page, err)
	}
	page, _ = s.ListSnapshots(ctx, store.ListOptions{Limit: 2, BeforeID: page[1].ID})
	if len(page) != 1 || page[0].ID != ids[0] {
		t.Fatalf("page 2 = %+v", page)
	}
}

func TestMigrationsUpDownUp(t *testing.T) {
	ctx := context.Background()
	pool := freshDB(t, true)
	m, err := store.NewMigrator(pool, store.Schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err := m.DownTo(ctx, 0); err != nil {
		t.Fatalf("down-to 0: %v", err)
	}
	if _, err := m.Up(ctx); err != nil {
		t.Fatalf("up again: %v", err)
	}
	if pending, _ := m.HasPending(ctx); pending {
		t.Fatal("pending migrations after up")
	}
}

func TestSeedsMirrorFixture(t *testing.T) {
	ctx := context.Background()
	pool := freshDB(t, true)
	s := store.New(pool)
	if err := store.SeedUp(ctx, pool, nil); err != nil {
		t.Fatalf("seed up: %v", err)
	}
	seed, err := s.LatestSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	crawled := crawlFixture(t, 2)

	// The seed must describe the same scenario the crawler produces from
	// testdata: same stats, status, skipped ids, integrity and gap index.
	if seed.Trigger != "seed" || seed.Status != crawled.Status {
		t.Errorf("seed trigger=%s status=%s, crawl status=%s", seed.Trigger, seed.Status, crawled.Status)
	}
	opts := cmp.Options{cmpopts.EquateEmpty(), cmpopts.IgnoreFields(model.MissingItem{}, "ErrorReason")}
	if diff := cmp.Diff(crawled.Stats, seed.Stats); diff != "" {
		t.Errorf("stats (-crawl +seed):\n%s", diff)
	}
	if diff := cmp.Diff(crawled.SkippedIDs, seed.SkippedIDs); diff != "" {
		t.Errorf("skipped (-crawl +seed):\n%s", diff)
	}
	for i := range crawled.Stories {
		c, sd := crawled.Stories[i], seed.Stories[i]
		if diff := cmp.Diff(c.Integrity, sd.Integrity); diff != "" {
			t.Errorf("story %d integrity (-crawl +seed):\n%s", i+1, diff)
		}
		if diff := cmp.Diff(c.MissingItems, sd.MissingItems, opts...); diff != "" {
			t.Errorf("story %d missing (-crawl +seed):\n%s", i+1, diff)
		}
	}
	for name, a := range crawled.Authors {
		if sa := seed.Authors[name]; sa == nil || sa.Status != a.Status {
			t.Errorf("seed author %s = %+v, want status %s", name, sa, a.Status)
		}
	}

	// A real crawl becomes latest immediately (seed finished_at is in 2000).
	id := save(t, s, crawlFixture(t, 1))
	if latestID, _ := s.LatestSnapshotID(ctx); latestID != id {
		t.Errorf("latest = %d, want real crawl %d", latestID, id)
	}

	m, _ := store.NewMigrator(pool, store.Seeds, nil)
	defer m.Close()
	if _, err := m.Down(ctx); err != nil {
		t.Fatalf("seed down: %v", err)
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM snapshots WHERE triggered_by = 'seed'`).Scan(&n)
	if n != 0 {
		t.Fatalf("%d seed snapshots after down", n)
	}
	if _, err := m.Up(ctx); err != nil {
		t.Fatalf("seed up again: %v", err)
	}
}

func TestRetentionKeepsSeedAndNewest(t *testing.T) {
	ctx := context.Background()
	pool := freshDB(t, true)
	s := store.New(pool)
	if err := store.SeedUp(ctx, pool, nil); err != nil {
		t.Fatal(err)
	}
	snap := crawlFixture(t, 1)
	var last int64
	for range 4 {
		cp := *snap
		last = save(t, s, &cp)
	}
	deleted, err := s.ApplyRetention(ctx, 2, 0)
	if err != nil || deleted != 2 {
		t.Fatalf("deleted = %d, %v; want 2", deleted, err)
	}
	var seeds, total, orphans int
	_ = pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE triggered_by = 'seed'), count(*) FROM snapshots`).Scan(&seeds, &total)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM items WHERE snapshot_id NOT IN (SELECT id FROM snapshots)`).Scan(&orphans)
	if seeds != 1 || total != 3 || orphans != 0 {
		t.Fatalf("seeds=%d total=%d orphans=%d", seeds, total, orphans)
	}
	if id, _ := s.LatestSnapshotID(ctx); id != last {
		t.Fatalf("latest = %d, want %d", id, last)
	}
	// Age rule: everything non-seed is younger than an hour, nothing goes.
	if n, err := s.ApplyRetention(ctx, 0, time.Hour); err != nil || n != 0 {
		t.Fatalf("age retention deleted %d, %v", n, err)
	}
}

func TestSeedRefusesOutdatedSchema(t *testing.T) {
	pool := freshDB(t, false)
	if err := store.SeedUp(context.Background(), pool, nil); !errors.Is(err, store.ErrSchemaNotCurrent) {
		t.Fatalf("err = %v, want ErrSchemaNotCurrent", err)
	}
}

func TestCrawlLockIsExclusive(t *testing.T) {
	ctx := context.Background()
	pool := freshDB(t, true)
	a, b := store.New(pool), store.New(pool) // two "replicas" sharing the DB
	unlock, ok, err := a.TryCrawlLock(ctx)
	if err != nil || !ok {
		t.Fatalf("first lock: ok=%v err=%v", ok, err)
	}
	if _, ok, err := b.TryCrawlLock(ctx); err != nil || ok {
		t.Fatalf("second lock while held: ok=%v err=%v", ok, err)
	}
	unlock()
	unlock2, ok, err := b.TryCrawlLock(ctx)
	if err != nil || !ok {
		t.Fatalf("lock after release: ok=%v err=%v", ok, err)
	}
	unlock2()
}
