-- Schema mirrors the API output (internal/model). Every row is scoped to a
-- snapshot so each crawl is stored exactly as observed.
-- Status columns are TEXT + CHECK (rather than enums) so they can be written
-- with COPY without registering custom types on every connection.

-- +goose Up
CREATE TABLE snapshots (
    id                 BIGSERIAL   PRIMARY KEY,
    status             TEXT        NOT NULL DEFAULT 'running'
                       CHECK (status IN ('running', 'completed', 'partial', 'failed')),
    triggered_by       TEXT        NOT NULL CHECK (triggered_by IN ('schedule', 'manual', 'cli', 'seed', 'test')),
    started_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at        TIMESTAMPTZ,
    duration_ms        BIGINT,
    stories_requested  SMALLINT    NOT NULL,
    skipped_ids        JSONB       NOT NULL DEFAULT '[]',   -- [{id, hn_rank, reason}]
    items_fetched      INTEGER     NOT NULL DEFAULT 0,
    authors_fetched    INTEGER     NOT NULL DEFAULT 0,
    missing_items      INTEGER     NOT NULL DEFAULT 0,
    failed_items       INTEGER     NOT NULL DEFAULT 0,
    error              TEXT
);
CREATE INDEX snapshots_latest_idx ON snapshots (finished_at DESC, id DESC)
    WHERE status IN ('completed', 'partial');

CREATE TABLE snapshot_stories (
    snapshot_id           BIGINT   NOT NULL REFERENCES snapshots (id) ON DELETE CASCADE,
    rank                  SMALLINT NOT NULL,           -- our 1..N
    hn_rank               SMALLINT NOT NULL,           -- 1-based position in /topstories
    story_id              BIGINT   NOT NULL,
    reported_descendants  INTEGER,
    fetched_comments      INTEGER  NOT NULL,
    parts_fetched         INTEGER  NOT NULL DEFAULT 0,
    ok_count              INTEGER  NOT NULL,
    deleted_count         INTEGER  NOT NULL,
    dead_count            INTEGER  NOT NULL,
    missing_count         INTEGER  NOT NULL,
    error_count           INTEGER  NOT NULL,
    max_depth             SMALLINT NOT NULL,
    complete              BOOLEAN  NOT NULL,
    PRIMARY KEY (snapshot_id, rank),
    UNIQUE (snapshot_id, story_id)
);

CREATE TABLE items (
    snapshot_id   BIGINT      NOT NULL REFERENCES snapshots (id) ON DELETE CASCADE,
    id            BIGINT      NOT NULL,        -- HN item id
    story_id      BIGINT      NOT NULL,        -- root story this node belongs to
    parent_id     BIGINT,                      -- NULL for the story itself
    depth         SMALLINT    NOT NULL,
    position      INTEGER     NOT NULL,        -- index among siblings (HN kids/parts order)
    relation      TEXT        NOT NULL CHECK (relation IN ('root', 'kid', 'part')),
    path          BIGINT[]    NOT NULL,        -- root..self
    status        TEXT        NOT NULL CHECK (status IN ('ok', 'deleted', 'dead', 'missing', 'error')),
    type          TEXT,                        -- story | comment | poll | pollopt | job (NULL if missing/error)
    author        TEXT,                        -- HN `by`
    posted_at     TIMESTAMPTZ,                 -- HN `time`
    title         TEXT,
    url           TEXT,
    text          TEXT,
    score         INTEGER,
    descendants   INTEGER,
    kids_count    INTEGER     NOT NULL DEFAULT 0,
    error_reason  TEXT,
    PRIMARY KEY (snapshot_id, id)
);
CREATE INDEX items_tree_idx   ON items (snapshot_id, story_id, depth, parent_id, relation, position);
CREATE INDEX items_author_idx ON items (snapshot_id, author);
CREATE INDEX items_gaps_idx   ON items (snapshot_id, story_id) WHERE status <> 'ok';

CREATE TABLE authors (
    snapshot_id      BIGINT      NOT NULL REFERENCES snapshots (id) ON DELETE CASCADE,
    username         TEXT        NOT NULL,     -- case-sensitive, stored as-is
    status           TEXT        NOT NULL CHECK (status IN ('ok', 'missing', 'error')),
    created          TIMESTAMPTZ,
    karma            INTEGER,
    about            TEXT,
    submitted_count  INTEGER,
    error_reason     TEXT,
    PRIMARY KEY (snapshot_id, username)
);

CREATE VIEW missing_items AS
SELECT snapshot_id, story_id, id, status, relation, parent_id, depth, path, error_reason
FROM items
WHERE status <> 'ok';

-- +goose Down
DROP VIEW  IF EXISTS missing_items;
DROP TABLE IF EXISTS authors;
DROP TABLE IF EXISTS items;
DROP TABLE IF EXISTS snapshot_stories;
DROP TABLE IF EXISTS snapshots;
