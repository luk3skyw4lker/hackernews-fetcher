package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
)

// LatestSnapshot loads the most recent completed or partial snapshot.
func (s *Store) LatestSnapshot(ctx context.Context) (*model.Snapshot, error) {
	id, err := s.LatestSnapshotID(ctx)
	if err != nil {
		return nil, err
	}
	return s.Snapshot(ctx, id)
}

// LatestSnapshotID returns the id of the most recent servable snapshot.
func (s *Store) LatestSnapshotID(ctx context.Context) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		SELECT id FROM snapshots WHERE status IN ('completed', 'partial')
		ORDER BY finished_at DESC, id DESC LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	return id, err
}

const summaryColumns = `id, status, triggered_by, started_at, finished_at, duration_ms, stories_requested,
	items_fetched, authors_fetched, missing_items, failed_items, COALESCE(error, '')`

func scanSummary(row pgx.Row, extra ...any) (model.SnapshotSummary, error) {
	var m model.SnapshotSummary
	dest := append([]any{&m.ID, &m.Status, &m.Trigger, &m.StartedAt, &m.FinishedAt, &m.DurationMS,
		&m.StoriesRequested, &m.Stats.ItemsFetched, &m.Stats.AuthorsFetched, &m.Stats.MissingItems,
		&m.Stats.FailedItems, &m.Error}, extra...)
	if err := row.Scan(dest...); err != nil {
		return m, err
	}
	m.StartedAt = m.StartedAt.UTC()
	m.FinishedAt = utcPtr(m.FinishedAt)
	return m, nil
}

// Snapshot loads one snapshot (any status) with its full trees and authors.
func (s *Store) Snapshot(ctx context.Context, id int64) (*model.Snapshot, error) {
	// REPEATABLE READ so the four queries see one consistent state even if
	// retention runs concurrently.
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // read-only

	var skipped []model.SkippedItem
	sum, err := scanSummary(tx.QueryRow(ctx, `SELECT `+summaryColumns+`, skipped_ids FROM snapshots WHERE id = $1`, id), &skipped)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("load snapshot %d: %w", id, err)
	}
	snap := &model.Snapshot{
		ID: sum.ID, Status: sum.Status, Trigger: sum.Trigger, StartedAt: sum.StartedAt,
		FinishedAt: sum.FinishedAt, StoriesRequested: sum.StoriesRequested, SkippedIDs: skipped,
		Stats: sum.Stats, Error: sum.Error, Stories: []*model.Story{}, Authors: map[string]*model.Author{},
	}
	if sum.DurationMS != nil {
		snap.DurationMS = *sum.DurationMS
	}
	if snap.SkippedIDs == nil {
		snap.SkippedIDs = []model.SkippedItem{}
	}

	if snap.Stories, err = loadStories(ctx, tx, id); err != nil {
		return nil, err
	}
	nodes, err := loadItems(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	roots := buildTrees(nodes)
	for _, st := range snap.Stories {
		st.Tree = roots[st.Tree.ID]
		if st.Tree == nil {
			return nil, fmt.Errorf("snapshot %d: story %d has no root item", id, st.Rank)
		}
		st.MissingItems = model.CollectMissing(st.Tree)
	}
	if snap.Authors, err = loadAuthors(ctx, tx, id); err != nil {
		return nil, err
	}
	return snap, nil
}

func loadStories(ctx context.Context, tx pgx.Tx, id int64) ([]*model.Story, error) {
	rows, err := tx.Query(ctx, `
		SELECT rank, hn_rank, story_id, reported_descendants, fetched_comments, parts_fetched, ok_count,
		       deleted_count, dead_count, missing_count, error_count, max_depth, complete
		FROM snapshot_stories WHERE snapshot_id = $1 ORDER BY rank`, id)
	if err != nil {
		return nil, err
	}
	stories := []*model.Story{}
	for rows.Next() {
		st := &model.Story{Tree: &model.Node{}}
		in := &st.Integrity
		if err := rows.Scan(&st.Rank, &st.HNRank, &st.Tree.ID, &in.ReportedDescendants, &in.FetchedComments,
			&in.PartsFetched, &in.OK, &in.Deleted, &in.Dead, &in.Missing, &in.Error, &in.MaxDepth, &in.Complete); err != nil {
			rows.Close()
			return nil, err
		}
		stories = append(stories, st)
	}
	return stories, rows.Err()
}

func loadItems(ctx context.Context, tx pgx.Tx, id int64) ([]*model.Node, error) {
	// Parents always have a lower depth, and siblings come out in HN order,
	// so buildTrees can attach nodes in a single pass.
	rows, err := tx.Query(ctx, `
		SELECT id, parent_id, depth, position, relation, path, status, COALESCE(type, ''),
		       COALESCE(author, ''), posted_at, COALESCE(title, ''), COALESCE(url, ''), COALESCE(text, ''),
		       score, descendants, kids_count, COALESCE(error_reason, '')
		FROM items WHERE snapshot_id = $1
		ORDER BY story_id, depth, parent_id, relation, position`, id)
	if err != nil {
		return nil, err
	}
	var nodes []*model.Node
	for rows.Next() {
		n := &model.Node{Children: []*model.Node{}}
		if err := rows.Scan(&n.ID, &n.ParentID, &n.Depth, &n.Position, &n.Relation, &n.Path, &n.Status, &n.Type,
			&n.By, &n.Time, &n.Title, &n.URL, &n.Text, &n.Score, &n.Descendants, &n.KidsCount, &n.ErrorReason); err != nil {
			rows.Close()
			return nil, err
		}
		n.Time = utcPtr(n.Time)
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

func loadAuthors(ctx context.Context, tx pgx.Tx, id int64) (map[string]*model.Author, error) {
	rows, err := tx.Query(ctx, `
		SELECT username, status, created, karma, COALESCE(about, ''), submitted_count, COALESCE(error_reason, '')
		FROM authors WHERE snapshot_id = $1`, id)
	if err != nil {
		return nil, err
	}
	authors := map[string]*model.Author{}
	for rows.Next() {
		a := &model.Author{}
		if err := rows.Scan(&a.ID, &a.Status, &a.Created, &a.Karma, &a.About, &a.SubmittedCount, &a.ErrorReason); err != nil {
			rows.Close()
			return nil, err
		}
		a.Created = utcPtr(a.Created)
		authors[a.ID] = a
	}
	return authors, rows.Err()
}

// ListOptions filters ListSnapshots.
type ListOptions struct {
	Limit    int
	BeforeID int64                // only ids < BeforeID (0 = no bound), for pagination
	Status   model.SnapshotStatus // "" = any
}

// ListSnapshots returns snapshot envelopes, newest first.
func (s *Store) ListSnapshots(ctx context.Context, o ListOptions) ([]model.SnapshotSummary, error) {
	if o.Limit <= 0 {
		o.Limit = 20
	}
	rows, err := s.pool.Query(ctx, `SELECT `+summaryColumns+` FROM snapshots
		WHERE ($1 = 0 OR id < $1) AND ($2 = '' OR status = $2)
		ORDER BY id DESC LIMIT $3`, o.BeforeID, string(o.Status), o.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.SnapshotSummary{}
	for rows.Next() {
		m, err := scanSummary(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
