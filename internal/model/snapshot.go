// Package model defines the output contract shared by the crawler, the
// Postgres store and the HTTP API.
package model

import "time"

// NodeStatus tells whether an item (or author) was retrieved, and if not, why.
type NodeStatus string

const (
	StatusOK      NodeStatus = "ok"      // fetched with content
	StatusDeleted NodeStatus = "deleted" // HN says the item was deleted
	StatusDead    NodeStatus = "dead"    // HN says the item is dead (flagged/killed)
	StatusMissing NodeStatus = "missing" // HN returned null: no such item/user (or not public)
	StatusError   NodeStatus = "error"   // we could not fetch it (network, 5xx, timeout, depth limit)
)

// SnapshotStatus is the lifecycle state of a crawl.
type SnapshotStatus string

const (
	SnapshotRunning   SnapshotStatus = "running"
	SnapshotCompleted SnapshotStatus = "completed" // every item fetched
	SnapshotPartial   SnapshotStatus = "partial"   // data produced, but some items/authors errored
	SnapshotFailed    SnapshotStatus = "failed"    // no usable data
)

// Relation is how a node hangs off its parent.
type Relation string

const (
	RelationRoot Relation = "root" // the story itself
	RelationKid  Relation = "kid"  // a comment (HN `kids`)
	RelationPart Relation = "part" // a poll option (HN `parts`)
)

// Snapshot is one complete crawl of the top stories.
type Snapshot struct {
	ID               int64              `json:"snapshot_id,omitempty"`
	Status           SnapshotStatus     `json:"status"`
	Trigger          string             `json:"trigger"`
	StartedAt        time.Time          `json:"started_at"`
	FinishedAt       *time.Time         `json:"finished_at,omitempty"`
	DurationMS       int64              `json:"duration_ms"`
	StoriesRequested int                `json:"stories_requested"`
	SkippedIDs       []SkippedItem      `json:"skipped_ids"`
	Stats            Stats              `json:"stats"`
	Error            string             `json:"error,omitempty"`
	Stories          []*Story           `json:"stories"`
	Authors          map[string]*Author `json:"authors"`
}

// SnapshotSummary is the snapshot envelope without stories or authors.
type SnapshotSummary struct {
	ID               int64          `json:"snapshot_id"`
	Status           SnapshotStatus `json:"status"`
	Trigger          string         `json:"trigger"`
	StartedAt        time.Time      `json:"started_at"`
	FinishedAt       *time.Time     `json:"finished_at,omitempty"`
	DurationMS       *int64         `json:"duration_ms,omitempty"`
	StoriesRequested int            `json:"stories_requested"`
	Stats            Stats          `json:"stats"`
	Error            string         `json:"error,omitempty"`
}

// SkippedItem is a /topstories entry that was not crawled (e.g. a job).
type SkippedItem struct {
	ID     int64  `json:"id"`
	HNRank int    `json:"hn_rank"`
	Reason string `json:"reason"`
}

// Stats are snapshot-wide counters.
type Stats struct {
	ItemsFetched   int `json:"items_fetched"`   // every node we attempted: stories, comments, poll options
	AuthorsFetched int `json:"authors_fetched"` // distinct usernames
	MissingItems   int `json:"missing_items"`   // nodes with status=missing
	FailedItems    int `json:"failed_items"`    // nodes with status=error
}

// Story is one top story with its thread and completeness report.
type Story struct {
	Rank         int           `json:"rank"`    // 1..N among crawled stories
	HNRank       int           `json:"hn_rank"` // 1-based position in /topstories
	Integrity    Integrity     `json:"integrity"`
	MissingItems []MissingItem `json:"missing_items"`
	Tree         *Node         `json:"tree"`
}

// Integrity summarises how complete a story's thread is.
// Status counters cover comments (relation=kid) only, so they compare
// like-for-like with HN's `descendants`. Poll options are in PartsFetched.
type Integrity struct {
	ReportedDescendants *int `json:"reported_descendants"`
	FetchedComments     int  `json:"fetched_comments"`
	PartsFetched        int  `json:"parts_fetched"`
	OK                  int  `json:"ok"`
	Deleted             int  `json:"deleted"`
	Dead                int  `json:"dead"`
	Missing             int  `json:"missing"`
	Error               int  `json:"error"`
	MaxDepth            int  `json:"max_depth"`
	// Complete is true when no node in the story (root, comments, parts) is
	// missing or errored. Deleted/dead are HN states, not gaps in our data.
	Complete bool `json:"complete"`
}

// MissingItem indexes a non-ok node so gaps can be found without walking the tree.
type MissingItem struct {
	ID          int64      `json:"id"`
	Status      NodeStatus `json:"status"`
	Relation    Relation   `json:"relation"`
	ParentID    *int64     `json:"parent_id"`
	Depth       int        `json:"depth"`
	Path        []int64    `json:"path"`
	ErrorReason string     `json:"error_reason,omitempty"`
}

// Node is a story, comment or poll option in the thread tree.
type Node struct {
	ID          int64      `json:"id"`
	Type        string     `json:"type,omitempty"`
	Status      NodeStatus `json:"status"`
	Relation    Relation   `json:"relation"`
	Depth       int        `json:"depth"`
	Position    int        `json:"position"`
	ParentID    *int64     `json:"parent_id"`
	Path        []int64    `json:"path"`
	By          string     `json:"by,omitempty"`
	Time        *time.Time `json:"time,omitempty"`
	Title       string     `json:"title,omitempty"`
	URL         string     `json:"url,omitempty"`
	Text        string     `json:"text,omitempty"`
	Score       *int       `json:"score,omitempty"`
	Descendants *int       `json:"descendants,omitempty"`
	KidsCount   int        `json:"kids_count"`
	ErrorReason string     `json:"error_reason,omitempty"`
	Children    []*Node    `json:"children"`
	Parts       []*Node    `json:"parts,omitempty"`
}

// Author is an HN user profile as seen during the crawl.
type Author struct {
	ID             string     `json:"id"`
	Status         NodeStatus `json:"status"` // ok | missing | error
	Created        *time.Time `json:"created,omitempty"`
	Karma          *int       `json:"karma,omitempty"`
	About          string     `json:"about,omitempty"`
	SubmittedCount *int       `json:"submitted_count,omitempty"`
	ErrorReason    string     `json:"error_reason,omitempty"`
}

// Summary returns the envelope of a snapshot.
func (s *Snapshot) Summary() SnapshotSummary {
	d := s.DurationMS
	return SnapshotSummary{
		ID: s.ID, Status: s.Status, Trigger: s.Trigger, StartedAt: s.StartedAt,
		FinishedAt: s.FinishedAt, DurationMS: &d, StoriesRequested: s.StoriesRequested,
		Stats: s.Stats, Error: s.Error,
	}
}
