package crawler_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/crawler"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/hn"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/hntest"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
)

func crawl(t *testing.T, srv *hntest.Server, opts crawler.Options, maxConc int) *model.Snapshot {
	t.Helper()
	client := hn.New(hn.Options{BaseURL: srv.URL, MaxConcurrency: maxConc, RequestTimeout: time.Second, MaxRetries: 1, BackoffBase: time.Millisecond})
	snap, err := crawler.New(client, opts, nil).Crawl(context.Background(), "test")
	if err != nil {
		t.Fatalf("Crawl: %v", err)
	}
	return snap
}

func byID(root *model.Node) map[int64]*model.Node {
	m := map[int64]*model.Node{}
	model.Walk(root, func(n *model.Node) { m[n.ID] = n })
	return m
}

func ids(nodes []*model.Node) []int64 {
	out := make([]int64, len(nodes))
	for i, n := range nodes {
		out[i] = n.ID
	}
	return out
}

func TestCrawlFixture(t *testing.T) {
	srv := hntest.New(t)
	snap := crawl(t, srv, crawler.Options{TopStories: 2}, 8)

	if snap.Status != model.SnapshotPartial {
		t.Errorf("status = %s, want partial (item 104 errors)", snap.Status)
	}
	if diff := cmp.Diff([]model.SkippedItem{{ID: 900, HNRank: 2, Reason: "job"}}, snap.SkippedIDs); diff != "" {
		t.Errorf("skipped_ids (-want +got):\n%s", diff)
	}
	if len(snap.Stories) != 2 {
		t.Fatalf("got %d stories, want 2", len(snap.Stories))
	}
	s1, s2 := snap.Stories[0], snap.Stories[1]
	if s1.Rank != 1 || s1.HNRank != 1 || s1.Tree.ID != 100 {
		t.Errorf("story 1 = rank %d hn_rank %d id %d", s1.Rank, s1.HNRank, s1.Tree.ID)
	}
	if s2.Rank != 2 || s2.HNRank != 3 || s2.Tree.ID != 200 {
		t.Errorf("story 2 = rank %d hn_rank %d id %d", s2.Rank, s2.HNRank, s2.Tree.ID)
	}

	// Thread shape, order and depth.
	n := byID(s1.Tree)
	if got := ids(s1.Tree.Children); !cmp.Equal(got, []int64{101, 102, 103, 104}) {
		t.Errorf("story 100 children order = %v", got)
	}
	if got := ids(n[101].Children); !cmp.Equal(got, []int64{105, 106}) {
		t.Errorf("101 children = %v", got)
	}
	if n[107].Depth != 3 || *n[107].ParentID != 105 || !cmp.Equal(n[107].Path, []int64{100, 101, 105, 107}) {
		t.Errorf("107 = depth %d parent %d path %v", n[107].Depth, *n[107].ParentID, n[107].Path)
	}
	if n[108].Depth != 3 || *n[108].ParentID != 106 {
		t.Errorf("108 under deleted 106 = depth %d parent %d", n[108].Depth, *n[108].ParentID)
	}

	// Statuses.
	want := map[int64]model.NodeStatus{
		100: model.StatusOK, 101: model.StatusOK, 102: model.StatusDead, 103: model.StatusMissing,
		104: model.StatusError, 105: model.StatusOK, 106: model.StatusDeleted, 107: model.StatusOK, 108: model.StatusOK,
	}
	for id, st := range want {
		if n[id].Status != st {
			t.Errorf("item %d status = %s, want %s", id, n[id].Status, st)
		}
	}
	if n[104].ErrorReason == "" {
		t.Error("error node 104 has no error_reason")
	}

	// Authors: every author, deleted item has none.
	if n[106].By != "" || n[101].By != "bob" {
		t.Errorf("authors: 106=%q 101=%q", n[106].By, n[101].By)
	}

	// Integrity.
	wantIn := model.Integrity{
		ReportedDescendants: ptr(7), FetchedComments: 8, OK: 4, Deleted: 1, Dead: 1, Missing: 1, Error: 1, MaxDepth: 3, Complete: false,
	}
	if diff := cmp.Diff(wantIn, s1.Integrity); diff != "" {
		t.Errorf("story 100 integrity (-want +got):\n%s", diff)
	}
	wantIn2 := model.Integrity{ReportedDescendants: ptr(3), FetchedComments: 3, PartsFetched: 2, OK: 3, MaxDepth: 2, Complete: true}
	if diff := cmp.Diff(wantIn2, s2.Integrity); diff != "" {
		t.Errorf("story 200 integrity (-want +got):\n%s", diff)
	}

	// Missing-items index with paths, in display order.
	var gotMissing []int64
	for _, m := range s1.MissingItems {
		gotMissing = append(gotMissing, m.ID)
	}
	// Display (depth-first) order: 106 lives under 101, so it precedes 102.
	if !cmp.Equal(gotMissing, []int64{106, 102, 103, 104}) {
		t.Errorf("missing_items = %v", gotMissing)
	}
	if len(s2.MissingItems) != 0 {
		t.Errorf("story 200 missing_items = %v", s2.MissingItems)
	}

	// Poll parts are separate from comments.
	if got := ids(s2.Tree.Parts); !cmp.Equal(got, []int64{210, 211}) {
		t.Errorf("poll parts = %v", got)
	}
	if s2.Tree.Parts[0].Relation != model.RelationPart || s2.Tree.Parts[0].Depth != 1 {
		t.Errorf("part = %+v", s2.Tree.Parts[0])
	}

	// Authors map.
	wantAuthors := map[string]model.NodeStatus{
		"alice": model.StatusOK, "bob": model.StatusOK, "carol": model.StatusOK, "dave": model.StatusOK,
		"eve": model.StatusOK, "frank": model.StatusOK, "ghost": model.StatusMissing,
	}
	if len(snap.Authors) != len(wantAuthors) {
		t.Errorf("authors = %d, want %d", len(snap.Authors), len(wantAuthors))
	}
	for name, st := range wantAuthors {
		if a := snap.Authors[name]; a == nil || a.Status != st {
			t.Errorf("author %s = %+v, want status %s", name, a, st)
		}
	}
	if _, ok := snap.Authors["gina"]; ok {
		t.Error("author of skipped job should not be fetched")
	}
	if *snap.Authors["alice"].SubmittedCount != 3 {
		t.Errorf("alice submitted_count = %d", *snap.Authors["alice"].SubmittedCount)
	}

	wantStats := model.Stats{ItemsFetched: 15, AuthorsFetched: 7, MissingItems: 1, FailedItems: 1}
	if diff := cmp.Diff(wantStats, snap.Stats); diff != "" {
		t.Errorf("stats (-want +got):\n%s", diff)
	}
}

func TestAuthorsFetchedOnce(t *testing.T) {
	srv := hntest.New(t)
	crawl(t, srv, crawler.Options{TopStories: 3}, 16)
	for _, u := range []string{"alice", "bob", "carol", "frank"} {
		if h := srv.Hits("/user/" + u + ".json"); h != 1 {
			t.Errorf("user %s fetched %d times, want 1", u, h)
		}
	}
}

func TestEachItemFetchedOnce(t *testing.T) {
	srv := hntest.New(t)
	crawl(t, srv, crawler.Options{TopStories: 2}, 16)
	for _, id := range []string{"100", "101", "105", "107", "200", "210"} {
		if h := srv.Hits("/item/" + id + ".json"); h != 1 {
			t.Errorf("item %s fetched %d times, want 1", id, h)
		}
	}
}

func TestConcurrencyIsBounded(t *testing.T) {
	srv := hntest.New(t)
	srv.Latency = 10 * time.Millisecond
	crawl(t, srv, crawler.Options{TopStories: 3}, 3)
	if m := srv.MaxInFlight.Load(); m > 3 {
		t.Fatalf("max in-flight = %d, want <= 3", m)
	}
	if m := srv.MaxInFlight.Load(); m < 2 {
		t.Fatalf("max in-flight = %d; crawl does not look concurrent", m)
	}
}

func TestIncludeJobs(t *testing.T) {
	srv := hntest.New(t)
	snap := crawl(t, srv, crawler.Options{TopStories: 2, IncludeJobs: true}, 8)
	if len(snap.SkippedIDs) != 0 || snap.Stories[1].Tree.ID != 900 || snap.Stories[1].Tree.Type != "job" {
		t.Fatalf("expected job 900 as story 2, got %+v / skipped %v", snap.Stories[1].Tree, snap.SkippedIDs)
	}
}

func TestMaxDepthMarksUnfetchedAsError(t *testing.T) {
	srv := hntest.New(t)
	snap := crawl(t, srv, crawler.Options{TopStories: 1, MaxDepth: 2}, 8)
	n := byID(snap.Stories[0].Tree)
	if n[107].Status != model.StatusError || !strings.Contains(n[107].ErrorReason, "MAX_DEPTH") {
		t.Fatalf("107 = %+v", n[107])
	}
	if srv.Hits("/item/107.json") != 0 {
		t.Fatal("item beyond MAX_DEPTH was fetched")
	}
}

func TestCrawlTimeoutProducesPartial(t *testing.T) {
	srv := hntest.New(t)
	srv.Delay("/item/105.json", 2*time.Second)
	client := hn.New(hn.Options{BaseURL: srv.URL, RequestTimeout: 5 * time.Second, BackoffBase: time.Millisecond})
	snap, err := crawler.New(client, crawler.Options{TopStories: 1, Timeout: 300 * time.Millisecond}, nil).Crawl(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	n := byID(snap.Stories[0].Tree)
	if snap.Status != model.SnapshotPartial || n[105].Status != model.StatusError || !strings.Contains(n[105].ErrorReason, "timeout") {
		t.Fatalf("status=%s 105=%+v", snap.Status, n[105])
	}
	if snap.Error == "" {
		t.Error("snapshot error should mention timeout")
	}
}

func TestTopStoriesFailureIsAnError(t *testing.T) {
	srv := hntest.New(t)
	srv.FailAlways("/topstories.json")
	client := hn.New(hn.Options{BaseURL: srv.URL, BackoffBase: time.Millisecond})
	if _, err := crawler.New(client, crawler.Options{TopStories: 1}, nil).Crawl(context.Background(), "test"); err == nil {
		t.Fatal("expected error")
	}
}

func TestRenderText(t *testing.T) {
	srv := hntest.New(t)
	snap := crawl(t, srv, crawler.Options{TopStories: 2}, 8)
	var buf bytes.Buffer
	if err := model.RenderText(&buf, snap); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		"skipped #900 at HN rank 2: job",
		"#106 [deleted]",
		"#103 [missing]",
		"#104 [error",
		"#102 eve [dead]",
		"Exactly — and mark them as deleted.",
		"Channels (30 votes)",
		"ghost [missing]",
		"INCOMPLETE",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("text output missing %q\n%s", want, out)
		}
	}
}

func ptr(i int) *int { return &i }

// panicky wraps the real client and panics on chosen items/users, simulating a
// bug inside a worker.
type panicky struct {
	*hn.Client
	itemID int64
	user   string
}

func (p panicky) Item(ctx context.Context, id int64) (*hn.Item, error) {
	if id == p.itemID {
		panic("boom item")
	}
	return p.Client.Item(ctx, id)
}

func (p panicky) User(ctx context.Context, name string) (*hn.User, error) {
	if name == p.user {
		panic("boom user")
	}
	return p.Client.User(ctx, name)
}

func newClient(srv *hntest.Server) *hn.Client {
	return hn.New(hn.Options{BaseURL: srv.URL, MaxConcurrency: 8, RequestTimeout: time.Second, MaxRetries: 1, BackoffBase: time.Millisecond})
}

func TestPanicInTreeWorkerAbortsCrawl(t *testing.T) {
	srv := hntest.New(t)
	srv.Latency = 5 * time.Millisecond
	snap, err := crawler.New(panicky{Client: newClient(srv), itemID: 105}, crawler.Options{TopStories: 2}, nil).
		Crawl(context.Background(), "test")

	var pe *crawler.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want *PanicError", err)
	}
	if snap == nil {
		t.Fatal("aborted crawl must still return a snapshot")
	}
	if snap.Status != model.SnapshotPartial || !strings.Contains(snap.Error, "panicked") {
		t.Errorf("status=%s error=%q, want partial with panic error", snap.Status, snap.Error)
	}
	// The panicking slot is an explicit gap, never nil.
	n := byID(snap.Stories[0].Tree)
	if n[105] == nil || n[105].Status != model.StatusError {
		t.Fatalf("105 = %+v, want error placeholder", n[105])
	}
	model.Walk(snap.Stories[1].Tree, func(*model.Node) {}) // must not hit a nil node
	for name, a := range snap.Authors {
		if a == nil {
			t.Errorf("author %s is nil", name)
		}
	}
}

func TestPanicInAuthorWorkerAbortsCrawl(t *testing.T) {
	srv := hntest.New(t)
	snap, err := crawler.New(panicky{Client: newClient(srv), user: "alice"}, crawler.Options{TopStories: 1}, nil).
		Crawl(context.Background(), "test")
	var pe *crawler.PanicError
	if !errors.As(err, &pe) || snap == nil {
		t.Fatalf("err = %v snap = %v", err, snap)
	}
	if a := snap.Authors["alice"]; a == nil || a.Status != model.StatusError {
		t.Errorf("alice = %+v, want error placeholder", a)
	}
}

func TestCallerCancellationIsFatal(t *testing.T) {
	srv := hntest.New(t)
	srv.Delay("/item/105.json", 2*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)

	snap, err := crawler.New(newClient(srv), crawler.Options{TopStories: 1}, nil).Crawl(ctx, "test")
	if !errors.Is(err, context.Canceled) || snap == nil {
		t.Fatalf("err = %v snap = %v", err, snap)
	}
	if n := byID(snap.Stories[0].Tree); n[105].Status != model.StatusError || n[105].ErrorReason != "canceled" {
		t.Errorf("105 = %+v", n[105])
	}
	if snap.Status != model.SnapshotPartial {
		t.Errorf("status = %s, want partial (story 100 was fetched)", snap.Status)
	}
}
