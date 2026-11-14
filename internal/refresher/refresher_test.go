package refresher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/store"
)

// fakeStore is an in-memory Store. The lock is a plain flag so tests can
// simulate another replica holding it.
type fakeStore struct {
	mu        sync.Mutex
	locked    bool
	nextID    int64
	rows      map[int64]*model.Snapshot // saved snapshots
	status    map[int64]string
	reasons   map[int64]string
	saveErr   error
	retention int
}

func newFakeStore() *fakeStore {
	return &fakeStore{rows: map[int64]*model.Snapshot{}, status: map[int64]string{}, reasons: map[int64]string{}}
}

func (f *fakeStore) TryCrawlLock(context.Context) (func(), bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.locked {
		return nil, false, nil
	}
	f.locked = true
	return func() { f.mu.Lock(); f.locked = false; f.mu.Unlock() }, true, nil
}

func (f *fakeStore) CreateRunning(_ context.Context, _ string, _ int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	f.status[f.nextID] = "running"
	return f.nextID, nil
}

func (f *fakeStore) SaveSnapshot(_ context.Context, id int64, snap *model.Snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return f.saveErr
	}
	snap.ID = id
	f.rows[id] = snap
	f.status[id] = string(snap.Status)
	return nil
}

func (f *fakeStore) MarkFailed(_ context.Context, id int64, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status[id], f.reasons[id] = "failed", reason
	return nil
}

func (f *fakeStore) FailStaleRunning(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for id, s := range f.status {
		if s == "running" {
			f.status[id] = "failed"
			n++
		}
	}
	return n, nil
}

func (f *fakeStore) ApplyRetention(context.Context, int, time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retention++
	return 0, nil
}

func (f *fakeStore) LatestSnapshotID(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var best int64
	for id := range f.rows {
		best = max(best, id)
	}
	if best == 0 {
		return 0, store.ErrNotFound
	}
	return best, nil
}

func (f *fakeStore) Snapshot(_ context.Context, id int64) (*model.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.rows[id]; ok {
		return s, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeStore) statusOf(id int64) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status[id]
}

// fakeCrawler returns a scripted result; block (if set) holds each crawl
// until closed, so tests can observe the "running" state.
type fakeCrawler struct {
	calls atomic.Int32
	block chan struct{}
	fail  atomic.Bool
}

func (c *fakeCrawler) Crawl(ctx context.Context, trigger string) (*model.Snapshot, error) {
	c.calls.Add(1)
	if c.block != nil {
		select {
		case <-c.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if c.fail.Load() {
		return nil, errors.New("hn: /topstories unavailable")
	}
	return &model.Snapshot{Status: model.SnapshotCompleted, Trigger: trigger,
		Stories: []*model.Story{}, Authors: map[string]*model.Author{}, SkippedIDs: []model.SkippedItem{}}, nil
}

func newRefresher(c Crawler, s Store) *Refresher {
	return New(c, s, nil, Options{Interval: time.Hour, Enabled: true, StoriesRequested: 5, RetentionCount: 10}, nil)
}

func TestRunOnceStoresAndCaches(t *testing.T) {
	fs := newFakeStore()
	r := newRefresher(&fakeCrawler{}, fs)
	snap, err := r.RunOnce(context.Background(), TriggerCLI)
	if err != nil || snap == nil || snap.ID != 1 {
		t.Fatalf("RunOnce = %+v, %v", snap, err)
	}
	if got := r.Cache().Get(); got == nil || got.Snapshot.ID != 1 || len(got.JSON) == 0 {
		t.Fatalf("cache = %+v", got)
	}
	if fs.retention != 1 {
		t.Fatalf("retention ran %d times", fs.retention)
	}
	st := r.State()
	if st.LastStatus != "completed" || st.ConsecutiveFailures != 0 || st.LastFinishedAt == nil {
		t.Fatalf("state = %+v", st)
	}
}

func TestFailureKeepsLastGoodSnapshot(t *testing.T) {
	fs := newFakeStore()
	c := &fakeCrawler{}
	r := newRefresher(c, fs)
	if _, err := r.RunOnce(context.Background(), TriggerCLI); err != nil {
		t.Fatal(err)
	}
	c.fail.Store(true)
	for range 2 {
		if _, err := r.RunOnce(context.Background(), TriggerSchedule); err == nil {
			t.Fatal("expected error")
		}
	}
	if fs.statusOf(3) != "failed" {
		t.Fatalf("snapshot 3 status = %s", fs.statusOf(3))
	}
	if got := r.Cache().Get(); got.Snapshot.ID != 1 {
		t.Fatalf("cache moved to %d", got.Snapshot.ID)
	}
	st := r.State()
	if st.ConsecutiveFailures != 2 || st.LastStatus != "failed" || st.LastError == "" {
		t.Fatalf("state = %+v", st)
	}
	c.fail.Store(false)
	_, _ = r.RunOnce(context.Background(), TriggerSchedule)
	if r.State().ConsecutiveFailures != 0 {
		t.Fatal("failures not reset after success")
	}
}

func TestSaveErrorMarksFailed(t *testing.T) {
	fs := newFakeStore()
	fs.saveErr = errors.New("disk full")
	r := newRefresher(&fakeCrawler{}, fs)
	if _, err := r.RunOnce(context.Background(), TriggerCLI); err == nil {
		t.Fatal("expected error")
	}
	if fs.statusOf(1) != "failed" || r.Cache().Get() != nil {
		t.Fatalf("status=%s cache=%v", fs.statusOf(1), r.Cache().Get())
	}
}

func TestLockHeldElsewhereIsBusy(t *testing.T) {
	fs := newFakeStore()
	fs.locked = true // another replica is crawling
	c := &fakeCrawler{}
	r := newRefresher(c, fs)
	if _, err := r.RunOnce(context.Background(), TriggerCLI); !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	if c.calls.Load() != 0 {
		t.Fatal("crawled without the lock")
	}
}

func TestTriggerWhileRunningIsBusy(t *testing.T) {
	c := &fakeCrawler{block: make(chan struct{})}
	r := newRefresher(c, newFakeStore())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	waitFor(t, func() bool { return r.State().Running }) // startup crawl in progress
	if err := r.Trigger(); !errors.Is(err, ErrBusy) {
		t.Fatalf("Trigger while running = %v, want ErrBusy", err)
	}
	close(c.block)
	waitFor(t, func() bool { return !r.State().Running })
	cancel()
	<-done
}

func TestTriggersCollapse(t *testing.T) {
	c := &fakeCrawler{}
	r := newRefresher(c, newFakeStore())
	for range 3 { // queued before the loop starts: at most one runs
		if err := r.Trigger(); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	waitFor(t, func() bool { return c.calls.Load() == 2 }) // startup + one manual
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done
	if n := c.calls.Load(); n != 2 {
		t.Fatalf("%d crawls, want 2 (startup + 1 collapsed manual)", n)
	}
}

func TestRunMarksStaleRunningAndLoadsLatest(t *testing.T) {
	fs := newFakeStore()
	fs.rows[1] = &model.Snapshot{ID: 1, Status: model.SnapshotCompleted}
	fs.status[1] = "completed"
	fs.status[2] = "running" // left by a crashed process
	fs.nextID = 2
	c := &fakeCrawler{block: make(chan struct{})}
	r := newRefresher(c, fs)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	waitFor(t, func() bool { return r.State().Running })
	if fs.statusOf(2) != "failed" {
		t.Fatalf("stale snapshot status = %s", fs.statusOf(2))
	}
	if got := r.Cache().Get(); got == nil || got.Snapshot.ID != 1 {
		t.Fatalf("latest not loaded before first crawl: %+v", got)
	}
	cancel() // shutdown mid-crawl: the row is recorded as failed
	<-done
	if fs.statusOf(3) != "failed" {
		t.Fatalf("interrupted crawl status = %s", fs.statusOf(3))
	}
}

func TestDisabledFollowerOnlySyncs(t *testing.T) {
	fs := newFakeStore()
	fs.rows[7] = &model.Snapshot{ID: 7, Status: model.SnapshotCompleted}
	c := &fakeCrawler{}
	r := New(c, fs, nil, Options{Interval: 20 * time.Millisecond, Enabled: false}, nil)
	if err := r.Trigger(); !errors.Is(err, ErrDisabled) {
		t.Fatalf("Trigger = %v, want ErrDisabled", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	waitFor(t, func() bool { g := r.Cache().Get(); return g != nil && g.Snapshot.ID == 7 })

	fs.mu.Lock()
	fs.rows[8] = &model.Snapshot{ID: 8, Status: model.SnapshotCompleted} // leader stored a new one
	fs.mu.Unlock()
	waitFor(t, func() bool { return r.Cache().Get().Snapshot.ID == 8 })
	cancel()
	<-done
	if c.calls.Load() != 0 {
		t.Fatal("follower crawled")
	}
}

func TestCacheIgnoresOlderSnapshot(t *testing.T) {
	var c Cache
	_ = c.Set(&model.Snapshot{ID: 5})
	_ = c.Set(&model.Snapshot{ID: 4})
	if c.Get().Snapshot.ID != 5 {
		t.Fatalf("cache rolled back to %d", c.Get().Snapshot.ID)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 2s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
