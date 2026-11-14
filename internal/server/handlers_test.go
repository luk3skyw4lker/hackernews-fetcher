package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/crawler"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/hn"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/hntest"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/refresher"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/store"
)

type fakeStore struct {
	pingErr error
	snaps   map[int64]*model.Snapshot
	listed  store.ListOptions
}

func (f *fakeStore) Ping(context.Context) error { return f.pingErr }

func (f *fakeStore) Snapshot(_ context.Context, id int64) (*model.Snapshot, error) {
	if s, ok := f.snaps[id]; ok {
		return s, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeStore) ListSnapshots(_ context.Context, o store.ListOptions) ([]model.SnapshotSummary, error) {
	f.listed = o
	out := []model.SnapshotSummary{}
	for i := range o.Limit {
		out = append(out, model.SnapshotSummary{ID: int64(100 - i), Status: model.SnapshotCompleted})
	}
	return out, nil
}

type fakeRefresher struct {
	err   error
	state refresher.State
	calls atomic.Int32
}

func (f *fakeRefresher) Trigger() error         { f.calls.Add(1); return f.err }
func (f *fakeRefresher) State() refresher.State { return f.state }

type fakeHN struct {
	err   error
	calls atomic.Int32
}

func (f *fakeHN) MaxItem(context.Context) (int64, error) { f.calls.Add(1); return 1, f.err }

// fixtureSnapshot crawls the testdata fixtures (3 stories) once per test.
func fixtureSnapshot(t *testing.T, id int64) *model.Snapshot {
	t.Helper()
	srv := hntest.New(t)
	client := hn.New(hn.Options{BaseURL: srv.URL, MaxConcurrency: 8, RequestTimeout: time.Second, MaxRetries: 1, BackoffBase: time.Millisecond})
	snap, err := crawler.New(client, crawler.Options{TopStories: 3}, nil).Crawl(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	snap.ID = id
	return snap
}

type env struct {
	srv   *Server
	store *fakeStore
	ref   *fakeRefresher
	hn    *fakeHN
	snap  *model.Snapshot
}

func newEnv(t *testing.T, withSnapshot bool) *env {
	t.Helper()
	e := &env{store: &fakeStore{snaps: map[int64]*model.Snapshot{}}, ref: &fakeRefresher{state: refresher.State{Enabled: true}}, hn: &fakeHN{}}
	cache := &refresher.Cache{}
	if withSnapshot {
		e.snap = fixtureSnapshot(t, 42)
		if err := cache.Set(e.snap); err != nil {
			t.Fatal(err)
		}
		old := fixtureSnapshot(t, 7)
		e.store.snaps[7] = old
	}
	e.srv = New(Deps{Store: e.store, Cache: cache, Refresher: e.ref, HN: e.hn, Version: "test", RefreshInterval: time.Minute})
	return e
}

func (e *env) do(t *testing.T, method, target string) (int, string, http.Header) {
	t.Helper()
	resp, err := e.srv.App.Test(httptest.NewRequest(method, target, nil), fiber.TestConfig{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func decode[T any](t *testing.T, body string) T {
	t.Helper()
	var v T
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
	return v
}

func errCode(t *testing.T, body string) string {
	return decode[errorBody](t, body).Error.Code
}

func TestTopStoriesBeforeFirstSnapshot(t *testing.T) {
	e := newEnv(t, false)
	code, body, _ := e.do(t, "GET", "/v1/top-stories")
	if code != 503 || errCode(t, body) != "no_snapshot" {
		t.Fatalf("%d %s", code, body)
	}
}

func TestTopStories(t *testing.T) {
	e := newEnv(t, true)
	code, body, hdr := e.do(t, "GET", "/v1/top-stories")
	if code != 200 || !strings.HasPrefix(hdr.Get("Content-Type"), "application/json") {
		t.Fatalf("%d %s", code, hdr.Get("Content-Type"))
	}
	got := decode[model.Snapshot](t, body)
	if got.ID != 42 || len(got.Stories) != 3 || len(got.Authors) == 0 {
		t.Fatalf("snapshot id=%d stories=%d authors=%d", got.ID, len(got.Stories), len(got.Authors))
	}

	_, body, _ = e.do(t, "GET", "/v1/top-stories?limit=1")
	one := decode[model.Snapshot](t, body)
	if len(one.Stories) != 1 || len(one.Authors) >= len(got.Authors) {
		t.Fatalf("limit=1: stories=%d authors=%d (full %d)", len(one.Stories), len(one.Authors), len(got.Authors))
	}

	code, body, hdr = e.do(t, "GET", "/v1/top-stories?format=text")
	if code != 200 || !strings.HasPrefix(hdr.Get("Content-Type"), "text/plain") || !strings.Contains(body, "depth") && !strings.Contains(body, "└") {
		t.Fatalf("text: %d %s\n%s", code, hdr.Get("Content-Type"), body)
	}
}

func TestQueryValidation(t *testing.T) {
	e := newEnv(t, true)
	for _, target := range []string{
		"/v1/top-stories?limit=0",
		"/v1/top-stories?limit=4", // only 3 stories in the snapshot
		"/v1/top-stories?limit=abc",
		"/v1/top-stories?format=xml",
		"/v1/snapshots?limit=101",
		"/v1/snapshots?status=weird",
		"/v1/snapshots?before_id=-1",
		"/v1/snapshots/abc",
		"/v1/snapshots/42/stories/zero",
	} {
		code, body, _ := e.do(t, "GET", target)
		if code != 400 || errCode(t, body) != "bad_request" {
			t.Errorf("%s: %d %s", target, code, body)
		}
	}
}

func TestListSnapshots(t *testing.T) {
	e := newEnv(t, true)
	code, body, _ := e.do(t, "GET", "/v1/snapshots?limit=2&before_id=50&status=partial")
	if code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	if e.store.listed != (store.ListOptions{Limit: 2, BeforeID: 50, Status: model.SnapshotPartial}) {
		t.Fatalf("options = %+v", e.store.listed)
	}
	resp := decode[struct {
		Snapshots    []model.SnapshotSummary `json:"snapshots"`
		NextBeforeID int64                   `json:"next_before_id"`
	}](t, body)
	if len(resp.Snapshots) != 2 || resp.NextBeforeID != 99 {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestSnapshotByID(t *testing.T) {
	e := newEnv(t, true)
	if code, body, _ := e.do(t, "GET", "/v1/snapshots/42"); code != 200 || decode[model.Snapshot](t, body).ID != 42 {
		t.Fatalf("cached: %d", code)
	}
	if code, body, _ := e.do(t, "GET", "/v1/snapshots/7"); code != 200 || decode[model.Snapshot](t, body).ID != 7 {
		t.Fatalf("from store: %d", code)
	}
	if code, body, _ := e.do(t, "GET", "/v1/snapshots/999"); code != 404 || errCode(t, body) != "not_found" {
		t.Fatalf("missing: %d %s", code, body)
	}
}

func TestStory(t *testing.T) {
	e := newEnv(t, true)
	st := e.snap.Stories[0]
	code, body, _ := e.do(t, "GET", "/v1/snapshots/42/stories/"+itoa(st.Tree.ID))
	if code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	resp := decode[struct {
		Story   model.Story              `json:"story"`
		Authors map[string]*model.Author `json:"authors"`
	}](t, body)
	want := model.AuthorsOf(e.snap.Authors, st.Tree)
	if resp.Story.Tree.ID != st.Tree.ID || len(resp.Authors) != len(want) {
		t.Fatalf("story %d authors %d want %d", resp.Story.Tree.ID, len(resp.Authors), len(want))
	}
	if code, _, _ := e.do(t, "GET", "/v1/snapshots/42/stories/123456789"); code != 404 {
		t.Fatalf("unknown story: %d", code)
	}
}

func TestAuthor(t *testing.T) {
	e := newEnv(t, true)
	var name string
	for n, a := range e.snap.Authors {
		if a.Status == model.StatusOK {
			name = n
			break
		}
	}
	code, body, _ := e.do(t, "GET", "/v1/authors/"+name)
	if code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	resp := decode[struct {
		Author  model.Author `json:"author"`
		ItemIDs []int64      `json:"item_ids"`
	}](t, body)
	if resp.Author.ID != name || len(resp.ItemIDs) == 0 {
		t.Fatalf("resp = %+v", resp)
	}
	if code, _, _ := e.do(t, "GET", "/v1/authors/nobody_here"); code != 404 {
		t.Fatalf("unknown author: %d", code)
	}
}

func TestRefresh(t *testing.T) {
	e := newEnv(t, true)
	if code, _, _ := e.do(t, "POST", "/v1/snapshots/refresh"); code != 202 {
		t.Fatalf("accepted: %d", code)
	}
	e.ref.err = refresher.ErrBusy
	if code, body, _ := e.do(t, "POST", "/v1/snapshots/refresh"); code != 409 || errCode(t, body) != "crawl_running" {
		t.Fatalf("busy: %d %s", code, body)
	}
	e.ref.err = refresher.ErrDisabled
	if code, _, _ := e.do(t, "POST", "/v1/snapshots/refresh"); code != 503 {
		t.Fatalf("disabled: %d", code)
	}
	e.ref.err = nil
	var last int
	for range 5 { // limit is 5/min per client; 3 used above
		last, _, _ = e.do(t, "POST", "/v1/snapshots/refresh")
	}
	if last != 429 {
		t.Fatalf("rate limit: last status %d", last)
	}
}

func TestNotFoundRoute(t *testing.T) {
	e := newEnv(t, true)
	code, body, _ := e.do(t, "GET", "/nope")
	if code != 404 || errCode(t, body) != "not_found" {
		t.Fatalf("%d %s", code, body)
	}
}

func TestLiveAndReady(t *testing.T) {
	e := newEnv(t, false)
	if code, _, _ := e.do(t, "GET", "/livez"); code != 200 {
		t.Fatalf("livez = %d", code)
	}
	if code, _, _ := e.do(t, "GET", "/readyz"); code != 503 {
		t.Fatalf("readyz without snapshot = %d", code)
	}

	e = newEnv(t, true)
	if code, _, _ := e.do(t, "GET", "/readyz"); code != 200 {
		t.Fatalf("readyz = %d", code)
	}
	e.store.pingErr = errors.New("connection refused")
	if code, _, _ := e.do(t, "GET", "/readyz"); code != 503 {
		t.Fatalf("readyz with db down = %d", code)
	}
	e.store.pingErr = nil
	e.hn.err = errors.New("hn down")
	if code, _, _ := e.do(t, "GET", "/readyz"); code != 200 {
		t.Fatalf("readyz must not depend on HN: %d", code)
	}

	e.srv.MarkShuttingDown()
	for _, p := range []string{"/livez", "/readyz"} {
		if code, _, _ := e.do(t, "GET", p); code != 503 {
			t.Fatalf("%s while shutting down = %d", p, code)
		}
	}
}

func TestHealthz(t *testing.T) {
	health := func(e *env) (int, healthResponse) {
		code, body, _ := e.do(t, "GET", "/healthz")
		return code, decode[healthResponse](t, body)
	}

	e := newEnv(t, true)
	if code, h := health(e); code != 200 || h.Status != "ok" || h.Checks.Refresher.LatestSnapshotID == nil {
		t.Fatalf("ok: %d %+v", code, h)
	}

	// HN down: degraded but still 200. The HN probe result is cached.
	e = newEnv(t, true)
	e.hn.err = errors.New("timeout")
	if code, h := health(e); code != 200 || h.Status != "degraded" || h.Checks.HNAPI.Status != "down" {
		t.Fatalf("hn down: %d %+v", code, h)
	}
	health(e)
	if n := e.hn.calls.Load(); n != 1 {
		t.Fatalf("HN probed %d times, want cached", n)
	}

	// Refresher failing: degraded.
	e = newEnv(t, true)
	e.ref.state = refresher.State{Enabled: true, ConsecutiveFailures: 2, LastStatus: "failed", LastError: "boom"}
	if _, h := health(e); h.Status != "degraded" || h.Checks.Refresher.Status != "failing" {
		t.Fatalf("refresher failing: %+v", h)
	}

	// Stale snapshot (older than 3x interval): degraded.
	e = newEnv(t, true)
	old := time.Now().Add(-time.Hour)
	e.snap.FinishedAt = &old
	if _, h := health(e); h.Status != "degraded" {
		t.Fatalf("stale: %+v", h)
	}

	// DB down or no snapshot: down, 503.
	e = newEnv(t, true)
	e.store.pingErr = errors.New("refused")
	if code, h := health(e); code != 503 || h.Status != "down" {
		t.Fatalf("db down: %d %+v", code, h)
	}
	if code, h := health(newEnv(t, false)); code != 503 || h.Status != "down" {
		t.Fatalf("no snapshot: %d %+v", code, h)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
