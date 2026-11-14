package refresher

import (
	"encoding/json"
	"sync/atomic"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
)

// Latest is the newest servable snapshot plus its pre-encoded JSON, so the
// hot path (GET /v1/top-stories without a limit) is a byte copy.
type Latest struct {
	Snapshot *model.Snapshot
	JSON     []byte
}

// Cache holds the latest snapshot. Reads are lock-free; writers swap the
// pointer, so readers always see a complete snapshot.
type Cache struct {
	p atomic.Pointer[Latest]
}

// Get returns the current snapshot, or nil before the first one exists.
func (c *Cache) Get() *Latest { return c.p.Load() }

// Set encodes snap and makes it the latest. A snapshot older than the
// current one (lower id) is ignored, so a slow loader can't roll it back.
func (c *Cache) Set(snap *model.Snapshot) error {
	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	next := &Latest{Snapshot: snap, JSON: b}
	for {
		cur := c.p.Load()
		if cur != nil && cur.Snapshot.ID > snap.ID {
			return nil
		}
		if c.p.CompareAndSwap(cur, next) {
			return nil
		}
	}
}
