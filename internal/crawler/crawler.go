// Package crawler builds a model.Snapshot of the HN top stories, walking every
// comment tree concurrently and fetching each author once.
//
// Error model:
//
//   - Item-level failures (an item 500s after retries, times out, an author
//     profile fails) are NOT returned to any errgroup. They become a node or
//     author with status "error" and an error_reason, and the goroutine returns
//     nil so the rest of the crawl continues. That is what lets the output show
//     exactly which items are missing.
//   - Crawl-level failures (/topstories fails, a worker panics, the caller's
//     context is cancelled) are fatal. A panic is returned from g.Go, which
//     cancels the shared context so every in-flight HN request aborts.
//
// Concurrency is bounded by the semaphore inside hn.Client, not by
// errgroup.SetLimit: goroutines spawn their children into the same group, and
// a limit there could deadlock with every slot held by a parent waiting to
// spawn.
package crawler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/hn"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
)

// HN is the subset of the HN client the crawler needs.
type HN interface {
	TopStories(ctx context.Context) ([]int64, error)
	Item(ctx context.Context, id int64) (*hn.Item, error)
	User(ctx context.Context, username string) (*hn.User, error)
}

// Options configures a crawl.
type Options struct {
	TopStories  int
	IncludeJobs bool
	MaxDepth    int // 0 = unlimited
	Timeout     time.Duration
}

// PanicError is returned when a crawl worker panics. It aborts the crawl.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("crawl worker panicked: %v", e.Value) }

// reasonAborted marks work that never finished because the crawl was aborted.
const reasonAborted = "not fetched: crawl aborted"

// Crawler is safe for concurrent use; each Crawl call is independent.
type Crawler struct {
	hn   HN
	opts Options
	log  *slog.Logger
}

// New returns a Crawler.
func New(client HN, opts Options, log *slog.Logger) *Crawler {
	if opts.TopStories <= 0 {
		opts.TopStories = 5
	}
	if log == nil {
		log = slog.Default()
	}
	return &Crawler{hn: client, opts: opts, log: log}
}

// Crawl fetches the top stories and returns a snapshot.
//
//   - (nil, err): /topstories failed; there is nothing to store.
//   - (snap, err): the crawl was aborted (worker panic or caller cancelled).
//     snap is still worth storing: Status is partial if any story was fetched,
//     failed otherwise, and Error explains why. Unfinished nodes are status
//     "error".
//   - (snap, nil): the crawl ran to the end. Hitting Options.Timeout is not
//     fatal: unfinished items are marked error and the snapshot is partial.
func (c *Crawler) Crawl(ctx context.Context, trigger string) (*model.Snapshot, error) {
	started := now()
	parent := ctx
	if c.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.opts.Timeout)
		defer cancel()
	}
	// abort cancels both groups at once; a panic in either one stops all work.
	ctx, abort := context.WithCancelCause(ctx)
	defer abort(nil)

	ids, err := c.hn.TopStories(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch topstories: %w", err)
	}

	treeG, tctx := errgroup.WithContext(ctx) // stories + comments, spawned recursively
	authG, actx := errgroup.WithContext(ctx) // author profiles, fanned out
	r := &run{c: c, abort: abort}
	r.authors = newAuthorFetcher(c.hn, actx, func(fn func()) { r.goSafe(authG, fn) })

	selected, skipped, selErr := r.selectStories(tctx, ids)

	stories := make([]*model.Story, len(selected))
	for i, sel := range selected {
		path := []int64{sel.id}
		stories[i] = &model.Story{
			Rank: i + 1, HNRank: sel.hnRank,
			Tree: placeholder(sel.id, nil, model.RelationRoot, 0, 0, path, reasonAborted),
		}
		r.goSafe(treeG, func() {
			stories[i].Tree = r.build(tctx, treeG, sel.id, sel.item, sel.err, nil, model.RelationRoot, 0, 0, path)
		})
	}

	treeErr := treeG.Wait() // every goroutine at every depth has finished
	// Safe: authors are requested only from tree goroutines, which have all
	// finished, so no authG.Go call can race with this Wait.
	authErr := authG.Wait()
	authors := r.authors.snapshot()

	for _, s := range stories {
		s.Integrity = model.ComputeIntegrity(s.Tree)
		s.MissingItems = model.CollectMissing(s.Tree)
	}
	finished := now()
	snap := &model.Snapshot{
		Trigger:          trigger,
		StartedAt:        started,
		FinishedAt:       &finished,
		DurationMS:       finished.Sub(started).Milliseconds(),
		StoriesRequested: c.opts.TopStories,
		SkippedIDs:       skipped,
		Stories:          stories,
		Authors:          authors,
	}
	snap.Stats = model.ComputeStats(stories, authors)
	snap.Status = model.DeriveStatus(snap.Stats, authors)

	fatal := errors.Join(selErr, treeErr, authErr)
	if fatal == nil && parent.Err() != nil {
		fatal = fmt.Errorf("crawl canceled: %w", context.Cause(parent))
	}
	switch {
	case fatal != nil:
		snap.Error = fatal.Error()
		snap.Status = model.SnapshotFailed
		if anyStoryFetched(stories) {
			snap.Status = model.SnapshotPartial
		}
		c.log.Error("crawl aborted", "status", snap.Status, "err", fatal)
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		snap.Error = fmt.Sprintf("crawl timeout after %s; unfinished items marked as error", c.opts.Timeout)
	}
	c.log.Info("crawl finished",
		"status", snap.Status, "stories", len(stories), "items", snap.Stats.ItemsFetched,
		"authors", snap.Stats.AuthorsFetched, "missing", snap.Stats.MissingItems,
		"failed", snap.Stats.FailedItems, "duration_ms", snap.DurationMS)
	return snap, fatal
}

type run struct {
	c       *Crawler
	abort   context.CancelCauseFunc
	authors *authorFetcher
	visited sync.Map // item id -> struct{}; guards against cycles/duplicates
}

// goSafe runs fn on g. fn records its own failures on the data and returns
// nothing; only a panic (a bug) becomes a returned error, and it also cancels
// the other group through abort.
func (r *run) goSafe(g *errgroup.Group, fn func()) {
	g.Go(func() (err error) {
		defer func() {
			if p := recover(); p != nil {
				pe := &PanicError{Value: p, Stack: debug.Stack()}
				r.c.log.Error("crawl worker panicked", "panic", p, "stack", string(pe.Stack))
				r.abort(pe)
				err = pe
			}
		}()
		fn()
		return nil
	})
}

type selection struct {
	id     int64
	hnRank int
	item   *hn.Item
	err    error
}

// selectStories walks /topstories in rank order and picks the first N items,
// skipping jobs unless configured otherwise. Items are fetched in concurrent
// batches; the fetched root items are reused by the tree walk. The returned
// error is non-nil only if a worker panicked.
func (r *run) selectStories(ctx context.Context, ids []int64) ([]selection, []model.SkippedItem, error) {
	want := r.c.opts.TopStories
	var selected []selection
	skipped := []model.SkippedItem{}
	next := 0
	for len(selected) < want && next < len(ids) && ctx.Err() == nil {
		n := min(want-len(selected), len(ids)-next)
		batch := make([]selection, n)
		var g errgroup.Group
		for i := range n {
			batch[i] = selection{id: ids[next+i], hnRank: next + i + 1, err: errors.New(reasonAborted)}
			r.goSafe(&g, func() {
				batch[i].item, batch[i].err = r.c.hn.Item(ctx, batch[i].id)
			})
		}
		err := g.Wait()
		next += n
		for _, b := range batch {
			if b.item != nil && b.item.Type == "job" && !r.c.opts.IncludeJobs {
				skipped = append(skipped, model.SkippedItem{ID: b.id, HNRank: b.hnRank, Reason: "job"})
				continue
			}
			// Missing/errored roots are kept: they are gaps the reader must see.
			selected = append(selected, b)
		}
		if err != nil {
			return selected, skipped, err
		}
	}
	return selected, skipped, nil
}

// build turns one fetched (or failed) item into a node and schedules its
// children on g (nested spawning into the same group). Each child goroutine
// writes only to its own slice slot, so the tree needs no locking and keeps
// HN's display order; g.Wait makes those writes visible to Crawl.
func (r *run) build(ctx context.Context, g *errgroup.Group, id int64, item *hn.Item, fetchErr error,
	parent *int64, rel model.Relation, depth, pos int, path []int64) *model.Node {

	node := &model.Node{
		ID: id, Relation: rel, Depth: depth, Position: pos, ParentID: parent,
		Path: path, Children: []*model.Node{},
	}
	if _, dup := r.visited.LoadOrStore(id, struct{}{}); dup {
		node.Status = model.StatusError
		node.ErrorReason = "duplicate item id in thread (cycle guard)"
		return node
	}
	switch {
	case fetchErr != nil: // non-fatal: recorded on the node, not returned
		node.Status, node.ErrorReason = model.StatusError, errReason(ctx, fetchErr)
		return node
	case item == nil:
		node.Status = model.StatusMissing
		return node
	case item.Deleted:
		node.Status = model.StatusDeleted
	case item.Dead:
		node.Status = model.StatusDead
	default:
		node.Status = model.StatusOK
	}
	fill(node, item)
	if parent != nil && rel == model.RelationKid && item.Parent != 0 && item.Parent != *parent {
		node.ErrorReason = fmt.Sprintf("parent mismatch: item says %d, reached from %d", item.Parent, *parent)
	}
	if item.By != "" {
		r.authors.request(item.By) // starts now, overlapping the tree walk
	}

	if len(item.Kids) > 0 {
		node.Children = make([]*model.Node, len(item.Kids))
		r.spawn(ctx, g, node, item.Kids, node.Children, model.RelationKid)
	}
	if len(item.Parts) > 0 {
		node.Parts = make([]*model.Node, len(item.Parts))
		r.spawn(ctx, g, node, item.Parts, node.Parts, model.RelationPart)
	}
	return node
}

func (r *run) spawn(ctx context.Context, g *errgroup.Group, parent *model.Node, ids []int64, slots []*model.Node, rel model.Relation) {
	pid := parent.ID
	depth := parent.Depth + 1
	if max := r.c.opts.MaxDepth; max > 0 && depth > max {
		for i, kid := range ids {
			slots[i] = placeholder(kid, &pid, rel, depth, i, append(slices.Clone(parent.Path), kid),
				fmt.Sprintf("not fetched: MAX_DEPTH=%d reached", max))
		}
		return
	}
	for i, kid := range ids {
		path := append(slices.Clone(parent.Path), kid)
		// Written before g.Go, overwritten by the goroutine; if the crawl is
		// aborted first, the slot is an explicit gap rather than nil.
		slots[i] = placeholder(kid, &pid, rel, depth, i, path, reasonAborted)
		r.goSafe(g, func() {
			it, err := r.c.hn.Item(ctx, kid)
			slots[i] = r.build(ctx, g, kid, it, err, &pid, rel, depth, i, path)
		})
	}
}

// placeholder is an error node for an item that was not (or not yet) fetched.
func placeholder(id int64, parent *int64, rel model.Relation, depth, pos int, path []int64, reason string) *model.Node {
	return &model.Node{
		ID: id, Status: model.StatusError, Relation: rel, Depth: depth, Position: pos,
		ParentID: parent, Path: path, Children: []*model.Node{}, ErrorReason: reason,
	}
}

func anyStoryFetched(stories []*model.Story) bool {
	for _, s := range stories {
		if s.Tree.Status != model.StatusError && s.Tree.Status != model.StatusMissing {
			return true
		}
	}
	return false
}

func fill(n *model.Node, it *hn.Item) {
	n.Type = it.Type
	n.By = it.By
	if it.Time > 0 {
		t := time.Unix(it.Time, 0).UTC()
		n.Time = &t
	}
	n.Title = it.Title
	n.URL = it.URL
	n.Text = it.Text
	n.Score = it.Score
	n.Descendants = it.Descendants
	n.KidsCount = len(it.Kids)
}

func errReason(ctx context.Context, err error) string {
	if ctx.Err() != nil {
		var pe *PanicError
		switch cause := context.Cause(ctx); {
		case errors.Is(cause, context.DeadlineExceeded):
			return "timeout: crawl deadline exceeded"
		case errors.As(cause, &pe):
			return "aborted: another crawl worker panicked"
		default:
			return "canceled"
		}
	}
	return err.Error()
}

// now returns the current time at Postgres (microsecond) precision so
// snapshots round-trip through the DB unchanged.
func now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }
