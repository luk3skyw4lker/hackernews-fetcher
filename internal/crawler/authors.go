package crawler

import (
	"context"
	"maps"
	"sync"
	"time"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
)

// authorFetcher fetches each username exactly once, no matter how many tree
// goroutines request it. Fetches run on their own errgroup (authG) as soon as
// an item reveals its author, overlapping with the rest of the tree walk.
//
// Failures are recorded on the Author (status missing/error), never returned
// to the group: only panics cancel the crawl.
type authorFetcher struct {
	hn    HN
	ctx   context.Context // authG's context
	spawn func(fn func()) // r.goSafe bound to authG

	mu      sync.Mutex
	results map[string]*model.Author
}

func newAuthorFetcher(client HN, ctx context.Context, spawn func(func())) *authorFetcher {
	return &authorFetcher{hn: client, ctx: ctx, spawn: spawn, results: map[string]*model.Author{}}
}

// request schedules a fetch of username unless it was already requested.
// Called only from tree goroutines, so every call happens before treeG.Wait
// returns, and therefore before authG.Wait is called.
func (a *authorFetcher) request(username string) {
	a.mu.Lock()
	if _, seen := a.results[username]; seen {
		a.mu.Unlock()
		return
	}
	// Claim it with a placeholder; overwritten when the fetch finishes. If the
	// worker never completes (panic elsewhere aborted the crawl), the author
	// still shows up as an explicit gap instead of a nil.
	a.results[username] = &model.Author{ID: username, Status: model.StatusError, ErrorReason: reasonAborted}
	a.mu.Unlock()

	a.spawn(func() {
		author := a.fetch(username)
		a.mu.Lock()
		a.results[username] = author
		a.mu.Unlock()
	})
}

func (a *authorFetcher) fetch(username string) *model.Author {
	u, err := a.hn.User(a.ctx, username)
	switch {
	case err != nil:
		return &model.Author{ID: username, Status: model.StatusError, ErrorReason: errReason(a.ctx, err)}
	case u == nil:
		return &model.Author{ID: username, Status: model.StatusMissing}
	}
	created := time.Unix(u.Created, 0).UTC()
	karma := u.Karma
	submitted := len(u.Submitted)
	return &model.Author{
		ID: username, Status: model.StatusOK, Created: &created,
		Karma: &karma, About: u.About, SubmittedCount: &submitted,
	}
}

// snapshot returns a copy of the results. Call only after authG.Wait.
func (a *authorFetcher) snapshot() map[string]*model.Author {
	a.mu.Lock()
	defer a.mu.Unlock()
	return maps.Clone(a.results)
}
