-- Demo snapshot for local development and offline demos.
-- Mirrors testdata/hn_fixture.json crawled with TOP_STORIES=2, and contains
-- every case the output must convey: a multi-level thread, a deleted comment
-- that still has replies, a dead comment, a missing item, an errored item,
-- a poll with options, a skipped job and an author whose profile is missing.
--
-- finished_at is in the past, so any real crawl becomes "latest" right away.
-- Ids come from BIGSERIAL (CTE + RETURNING), so sequences stay correct.

-- +goose Up
WITH snap AS (
    INSERT INTO snapshots (status, triggered_by, started_at, finished_at, duration_ms, stories_requested,
                           skipped_ids, items_fetched, authors_fetched, missing_items, failed_items)
    VALUES ('partial', 'seed', '2000-01-01T00:00:00Z', '2000-01-01T00:00:02Z', 2000, 2,
            '[{"id": 900, "hn_rank": 2, "reason": "job"}]', 15, 7, 1, 1)
    RETURNING id
),
stories AS (
    INSERT INTO snapshot_stories (snapshot_id, rank, hn_rank, story_id, reported_descendants, fetched_comments,
                                  parts_fetched, ok_count, deleted_count, dead_count, missing_count, error_count,
                                  max_depth, complete)
    SELECT snap.id, v.* FROM snap, (VALUES
        (1, 1, 100::bigint, 7, 8, 0, 4, 1, 1, 1, 1, 3, false),
        (2, 3, 200::bigint, 3, 3, 2, 3, 0, 0, 0, 0, 2, true)
    ) AS v
    RETURNING 1
),
seed_items AS (
    INSERT INTO items (snapshot_id, id, story_id, parent_id, depth, position, relation, path, status, type,
                       author, posted_at, title, url, text, score, descendants, kids_count, error_reason)
    SELECT snap.id, v.id, v.story_id, v.parent_id::bigint, v.depth, v.position, v.relation, v.path::bigint[],
           v.status, v.type, v.author, to_timestamp(v.posted_at), v.title, v.url, v.text, v.score::int,
           v.descendants::int, v.kids_count, v.error_reason
    FROM snap, (VALUES
        -- story 100: thread with every kind of gap
        (100::bigint, 100::bigint, NULL::bigint, 0, 0, 'root', '{100}',             'ok',      'story',   'alice', 1762956000, 'Show HN: A tiny HN crawler', 'https://example.com/crawler', NULL, 128, 7, 4, NULL),
        (101, 100, 100, 1, 0, 'kid',  '{100,101}',         'ok',      'comment', 'bob',   1762956100, NULL, NULL, 'Nice work! How do you handle <i>deleted</i> comments?', NULL, NULL, 2, NULL),
        (102, 100, 100, 1, 1, 'kid',  '{100,102}',         'dead',    'comment', 'eve',   1762956200, NULL, NULL, '[flagged]', NULL, NULL, 0, NULL),
        (103, 100, 100, 1, 2, 'kid',  '{100,103}',         'missing', NULL,      NULL,    NULL,       NULL, NULL, NULL, NULL, NULL, 0, NULL),
        (104, 100, 100, 1, 3, 'kid',  '{100,104}',         'error',   NULL,      NULL,    NULL,       NULL, NULL, NULL, NULL, NULL, 0, 'GET /item/104.json failed after 4 attempt(s): http 500'),
        (105, 100, 101, 2, 0, 'kid',  '{100,101,105}',     'ok',      'comment', 'carol', 1762956400, NULL, NULL, 'They keep their kids, so you keep walking.', NULL, NULL, 1, NULL),
        (106, 100, 101, 2, 1, 'kid',  '{100,101,106}',     'deleted', 'comment', NULL,    1762956500, NULL, NULL, NULL, NULL, NULL, 1, NULL),
        (107, 100, 105, 3, 0, 'kid',  '{100,101,105,107}', 'ok',      'comment', 'alice', 1762956600, NULL, NULL, 'Exactly &mdash; and mark them as deleted.', NULL, NULL, 0, NULL),
        (108, 100, 106, 3, 0, 'kid',  '{100,101,106,108}', 'ok',      'comment', 'dave',  1762956700, NULL, NULL, 'Replying to a deleted comment.', NULL, NULL, 0, NULL),
        -- story 200: poll with options (parts) and a reply by an author with no public profile
        (200, 200, NULL, 0, 0, 'root', '{200}',            'ok',      'poll',    'frank', 1762957000, 'Poll: Favorite Go concurrency primitive?', NULL, NULL, 64, 3, 2, NULL),
        (201, 200, 200, 1, 0, 'kid',  '{200,201}',         'ok',      'comment', 'bob',   1762957100, NULL, NULL, 'Channels, obviously.', NULL, NULL, 1, NULL),
        (202, 200, 200, 1, 1, 'kid',  '{200,202}',         'ok',      'comment', 'carol', 1762957200, NULL, NULL, 'errgroup is underrated.', NULL, NULL, 0, NULL),
        (203, 200, 201, 2, 0, 'kid',  '{200,201,203}',     'ok',      'comment', 'ghost', 1762957300, NULL, NULL, 'Mutexes are fine too.', NULL, NULL, 0, NULL),
        (210, 200, 200, 1, 0, 'part', '{200,210}',         'ok',      'pollopt', 'frank', 1762957000, NULL, NULL, 'Channels', 30, NULL, 0, NULL),
        (211, 200, 200, 1, 1, 'part', '{200,211}',         'ok',      'pollopt', 'frank', 1762957000, NULL, NULL, 'Mutexes', 12, NULL, 0, NULL)
    ) AS v (id, story_id, parent_id, depth, position, relation, path, status, type,
            author, posted_at, title, url, text, score, descendants, kids_count, error_reason)
    RETURNING 1
)
INSERT INTO authors (snapshot_id, username, status, created, karma, about, submitted_count)
SELECT snap.id, v.username, v.status, to_timestamp(v.created), v.karma, v.about, v.submitted_count
FROM snap, (VALUES
    ('alice', 'ok',      1300000000, 4200, 'Builds crawlers.',              3),
    ('bob',   'ok',      1310000000, 900,  NULL,                            2),
    ('carol', 'ok',      1320000000, 1500, NULL,                            2),
    ('dave',  'ok',      1330000000, 77,   NULL,                            2),
    ('eve',   'ok',      1340000000, -4,   NULL,                            1),
    ('frank', 'ok',      1350000000, 3100, 'Polls <b>everything</b>.',      3),
    ('ghost', 'missing', NULL,       NULL, NULL,                            NULL)
) AS v (username, status, created, karma, about, submitted_count);

-- +goose Down
DELETE FROM snapshots WHERE triggered_by = 'seed';
