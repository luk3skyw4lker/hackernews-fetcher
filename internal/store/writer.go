package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
)

// CreateRunning inserts a snapshot row in status "running" and commits it
// immediately, so a crash mid-crawl leaves a visible row (cleaned up as
// "failed" on the next start).
func (s *Store) CreateRunning(ctx context.Context, trigger string, storiesRequested int) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO snapshots (status, triggered_by, stories_requested) VALUES ('running', $1, $2) RETURNING id`,
		trigger, storiesRequested).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("create snapshot: %w", err)
	}
	return id, nil
}

// SaveSnapshot writes a crawled snapshot into the running row id, in one
// transaction: COPY items and authors, insert stories, then update the
// envelope. Readers only see completed/partial snapshots, so it appears
// atomically on commit. snap.ID is set to id.
func (s *Store) SaveSnapshot(ctx context.Context, id int64, snap *model.Snapshot) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"items"}, itemColumns, pgx.CopyFromRows(itemRows(id, snap))); err != nil {
		return fmt.Errorf("copy items: %w", err)
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"authors"}, authorColumns, pgx.CopyFromRows(authorRows(id, snap))); err != nil {
		return fmt.Errorf("copy authors: %w", err)
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"snapshot_stories"}, storyColumns, pgx.CopyFromRows(storyRows(id, snap))); err != nil {
		return fmt.Errorf("copy stories: %w", err)
	}
	skipped := snap.SkippedIDs
	if skipped == nil {
		skipped = []model.SkippedItem{}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE snapshots SET
			status = $2, triggered_by = $3, started_at = $4, finished_at = $5, duration_ms = $6,
			stories_requested = $7, skipped_ids = $8, items_fetched = $9, authors_fetched = $10,
			missing_items = $11, failed_items = $12, error = $13
		WHERE id = $1`,
		id, string(snap.Status), snap.Trigger, snap.StartedAt, snap.FinishedAt, snap.DurationMS,
		snap.StoriesRequested, skipped, snap.Stats.ItemsFetched, snap.Stats.AuthorsFetched,
		snap.Stats.MissingItems, snap.Stats.FailedItems, nullStr(snap.Error))
	if err != nil {
		return fmt.Errorf("update snapshot: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("update snapshot %d: %w", id, ErrNotFound)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	snap.ID = id
	return nil
}

// MarkFailed records a crawl that produced nothing worth storing.
func (s *Store) MarkFailed(ctx context.Context, id int64, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE snapshots SET status = 'failed', finished_at = now(),
			duration_ms = (EXTRACT(EPOCH FROM now() - started_at) * 1000)::bigint, error = $2
		WHERE id = $1`, id, reason)
	return err
}

// FailStaleRunning marks snapshots left "running" by a crashed or stopped
// process as failed. Call it on startup while holding the crawl lock.
func (s *Store) FailStaleRunning(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE snapshots SET status = 'failed', finished_at = now(),
			error = 'interrupted: process stopped before the crawl finished'
		WHERE status = 'running'`)
	return tag.RowsAffected(), err
}

// ApplyRetention keeps the newest keep snapshots and drops those older than
// maxAge (0 disables either rule). Seed snapshots belong to goose and are
// never deleted here; running rows are left alone. Child rows cascade.
func (s *Store) ApplyRetention(ctx context.Context, keep int, maxAge time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM snapshots
		WHERE triggered_by <> 'seed' AND status <> 'running' AND (
			($1::int > 0 AND id NOT IN (
				SELECT id FROM snapshots WHERE triggered_by <> 'seed' ORDER BY id DESC LIMIT $1::int))
			OR ($2::float8 > 0 AND started_at < now() - make_interval(secs => $2::float8))
		)`, keep, maxAge.Seconds())
	if err != nil {
		return 0, fmt.Errorf("retention: %w", err)
	}
	return tag.RowsAffected(), nil
}

var itemColumns = []string{
	"snapshot_id", "id", "story_id", "parent_id", "depth", "position", "relation", "path", "status", "type",
	"author", "posted_at", "title", "url", "text", "score", "descendants", "kids_count", "error_reason",
}

func itemRows(snapID int64, snap *model.Snapshot) [][]any {
	var rows [][]any
	seen := map[int64]bool{}
	for _, st := range snap.Stories {
		storyID := st.Tree.ID
		model.Walk(st.Tree, func(n *model.Node) {
			// (snapshot_id, id) is the primary key. HN never puts an item in
			// two places; if the crawler's cycle guard produced a duplicate
			// marker, the first occurrence is the one stored.
			if seen[n.ID] {
				return
			}
			seen[n.ID] = true
			rows = append(rows, []any{
				snapID, n.ID, storyID, n.ParentID, n.Depth, n.Position, string(n.Relation), n.Path,
				string(n.Status), nullStr(n.Type), nullStr(n.By), n.Time, nullStr(n.Title), nullStr(n.URL),
				nullStr(n.Text), n.Score, n.Descendants, n.KidsCount, nullStr(n.ErrorReason),
			})
		})
	}
	return rows
}

var authorColumns = []string{"snapshot_id", "username", "status", "created", "karma", "about", "submitted_count", "error_reason"}

func authorRows(snapID int64, snap *model.Snapshot) [][]any {
	rows := make([][]any, 0, len(snap.Authors))
	for name, a := range snap.Authors {
		rows = append(rows, []any{
			snapID, name, string(a.Status), a.Created, a.Karma, nullStr(a.About), a.SubmittedCount, nullStr(a.ErrorReason),
		})
	}
	return rows
}

var storyColumns = []string{
	"snapshot_id", "rank", "hn_rank", "story_id", "reported_descendants", "fetched_comments", "parts_fetched",
	"ok_count", "deleted_count", "dead_count", "missing_count", "error_count", "max_depth", "complete",
}

func storyRows(snapID int64, snap *model.Snapshot) [][]any {
	rows := make([][]any, 0, len(snap.Stories))
	for _, st := range snap.Stories {
		in := st.Integrity
		rows = append(rows, []any{
			snapID, st.Rank, st.HNRank, st.Tree.ID, in.ReportedDescendants, in.FetchedComments, in.PartsFetched,
			in.OK, in.Deleted, in.Dead, in.Missing, in.Error, in.MaxDepth, in.Complete,
		})
	}
	return rows
}

func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
