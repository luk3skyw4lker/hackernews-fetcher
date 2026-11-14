# HackerNews Top Stories Fetcher — Implementation Plan

## 1. Goal

Build a small Go service (Fiber v3) that fetches the **top 5 HackerNews stories**, each with its **complete comment tree**, plus the **profile of every author** involved (story authors and comment authors), fetching **concurrently**.

A **background refresher** crawls HackerNews on an interval and stores each run as a **snapshot in Postgres**, using a schema modeled on the API output. The HTTP API serves data **from our own database**, never straight from HackerNews, so we can also look back at earlier snapshots. A **CLI mode** runs a single crawl on demand.

The output alone must let a reader answer:

| Question | How the output answers it |
|---|---|
| What is the full thread of a story, with depth? | Each story is a recursive tree; every node carries `depth` (story = 0), `parent_id` and `position` (order among siblings, same as HN). |
| Who authored every post and comment? | Every node has `by`; a top-level `authors` map holds the full profile for each username. |
| Are any items missing from a thread? | Every node has a `status` (`ok`, `deleted`, `dead`, `missing`, `error`); each story has an `integrity` block comparing HN's `descendants` count with what we actually fetched, plus a flat `missing_items` list with the path to each gap. |

---

## 2. HackerNews API surface

Reference: the official docs (local copy in `../API/README.md`).
Base URL: `https://hacker-news.firebaseio.com/v0`. Every resource is `<path>.json`.

| Endpoint | Use |
|---|---|
| `GET /topstories.json` | Up to 500 IDs, ranked. **The list also contains jobs** (see "Choosing the top 5" below). |
| `GET /item/{id}.json` | Any item. All item kinds (`story`, `comment`, `job`, `poll`, `pollopt`) share this endpoint. |
| `GET /user/{id}.json` | User profile. |
| `GET /maxitem.json` | Returns a single integer. A cheap call, used by the HN health probe. |
| `GET /updates.json` | Recently changed item ids and profiles. Not used in v1; listed as a future optimization (§15). |

### Item fields (only `id` is required)

| Field | Type | Notes for us |
|---|---|---|
| `id` | int | Required. |
| `deleted` | bool | Item was deleted, so `by`/`text` are usually gone. → `status: deleted`. |
| `type` | string | `job` \| `story` \| `comment` \| `poll` \| `pollopt`. |
| `by` | string | Author username. **Case-sensitive.** Missing on deleted items. |
| `time` | int | **Unix seconds**. Converted to RFC 3339 in our output and `TIMESTAMPTZ` in Postgres. |
| `text` | string | **HTML**. Stored and returned raw. `format=text` strips tags and unescapes entities. |
| `dead` | bool | Flagged/killed. → `status: dead`. |
| `parent` | int | Parent comment or story. We cross-check it against the parent we came from and flag a mismatch in `error_reason`. |
| `poll` | int | For `pollopt`: the owning poll. |
| `kids` | []int | Child comment ids **in ranked display order**. Used as the tree order (`position`). |
| `url` | string | Story URL. Can be `""` (e.g. jobs, Ask HN), so it's normalized to `null`. |
| `score` | int | Story score, or votes for a `pollopt`. |
| `title` | string | **HTML**. Same handling as `text`. |
| `parts` | []int | For `poll`: the poll's option ids, in display order. **Not part of `kids`.** |
| `descendants` | int | For stories/polls: "the total comment count". This is HN's own number, which we compare against the comments we actually fetched. |

### User fields

| Field | Notes |
|---|---|
| `id` | Required. Case-sensitive, so it's stored as plain `TEXT` with no lowercasing or `citext`. |
| `created` | Required. Unix seconds. |
| `karma` | Required. |
| `about` | Optional. HTML. |
| `submitted` | List of item ids, often 10k+ entries. Reduced to `submitted_count`. |

"Only users that have public activity … are available through the API", so a `null` user response is expected occasionally and becomes `status: missing`.

### Behaviors the design must handle

- **`null` body**: the item or user doesn't exist or isn't public. → `missing`.
- **Deleted items can still have `kids`**, so we keep walking below them.
- **Network/5xx/timeout after retries** → `error`. This is kept separate from `missing` so a reader knows the failure was *ours*, not HN's.
- **`descendants` vs fetched count**: deleted/dead comments can make these differ. Both numbers are reported, and a mismatch alone isn't treated as an error.
- **Forward compatibility**: the docs say clients should ignore fields they don't expect. We decode with plain `encoding/json` (never `DisallowUnknownFields`), and unknown fields are ignored.
- **"There is currently no rate limit."** We still cap concurrency (§6.1) to be a good citizen.
- **Firebase realtime/SSE** is available, but HN recommends it mainly through client SDKs. Plain REST polling fits our snapshot model better (and Go has no official Firebase RTDB streaming client).

### Choosing the "top 5 stories"

`/topstories` mixes in `job` items, which have no comments. By default the crawler walks the ranked list in order and takes the **first 5 items whose `type` is `story` or `poll`**, skipping jobs (`TOP_STORIES_INCLUDE_JOBS=false`). The skipped ids are recorded on the snapshot (`skipped_ids`), and each story keeps its original HN rank (`hn_rank`) plus our `rank` (1..5), so the output shows exactly what was skipped and why. Setting the flag to `true` takes the first 5 ids as-is.

### Polls

For a `poll`, the `parts` (poll options) are fetched too, because their authors count as "authors involved". They are attached to the poll node as `parts: [...]` (with `relation: "part"`, `depth` = poll depth + 1) and kept separate from `children` (comments), so the comment tree stays a pure reply thread. They are counted in integrity as `parts_fetched` but not in `fetched_comments`, so the comparison with `descendants` stays like-for-like.

---

## 3. Architecture

```
                 ┌────────────────────────────────────────┐
  HTTP ─────────▶│ Fiber v3 app (handlers, middleware)    │
                 └───────────────┬────────────────────────┘
                                 │ reads
                 ┌───────────────▼────────────────────────┐
                 │ store.SnapshotReader                   │
                 │  + in-memory cache of latest snapshot  │
                 └───────────────┬────────────────────────┘
                                 │
                        ┌────────▼────────┐
                        │   PostgreSQL    │
                        └────────▲────────┘
                                 │ writes (one tx per snapshot)
                 ┌───────────────┴────────────────────────┐
  ticker ───────▶│ refresher (background goroutine)       │◀── POST /v1/snapshots/refresh
  CLI `crawl` ──▶│  - advisory lock (one crawler at once) │
                 └───────────────┬────────────────────────┘
                                 │
                 ┌───────────────▼────────────────────────┐
                 │ crawler.Crawler                        │
                 │  - top N stories, concurrent tree walk │
                 │  - deduplicated author fetch           │
                 └───────────────┬────────────────────────┘
                                 │
                 ┌───────────────▼────────────────────────┐
                 │ hn.Client (semaphore, retries, timeout)│
                 └────────────────────────────────────────┘
```

The design splits into three parts:

- **Crawler**: pure fetching. It builds an in-memory `Snapshot` and knows nothing about the DB or HTTP.
- **Store**: persists a `Snapshot` and loads it back. The output model is the contract between the two.
- **Server / refresher / CLI**: entry points that wire the two together.

### Project layout

```
.
├── cmd/
│   └── hnfetcher/
│       └── main.go             # subcommand dispatch: serve | crawl | snapshot | migrate | healthcheck
├── internal/
│   ├── config/config.go        # env-based config + validation
│   ├── hn/
│   │   ├── client.go           # HTTP client for the HN API
│   │   ├── models.go           # raw API structs
│   │   └── client_test.go
│   ├── crawler/
│   │   ├── crawler.go          # concurrent tree walk + integrity computation
│   │   ├── authors.go          # deduplicated author fetcher
│   │   └── crawler_test.go
│   ├── model/
│   │   ├── snapshot.go         # Snapshot, Story, Node, Author, Integrity, MissingItem, Status
│   │   └── render.go           # plain-text tree rendering
│   ├── store/
│   │   ├── postgres.go         # pgxpool setup, ping
│   │   ├── writer.go           # SaveSnapshot (COPY inside one tx)
│   │   ├── reader.go           # LatestSnapshot, Snapshot(id), ListSnapshots, Author, Story
│   │   ├── tree.go             # flat rows -> nested tree
│   │   ├── migrate.go          # goose providers for migrations + seeds (//go:embed)
│   │   ├── migrations/         # schema: goose SQL files (Up + Down in one file)
│   │   │   └── 00001_init.sql
│   │   ├── seeds/              # data: goose SQL files, separate version table
│   │   │   └── 00001_demo_snapshot.sql
│   │   ├── lock.go             # pg advisory crawl lock on a dedicated conn
│   │   └── store_test.go       # testcontainers-go Postgres (or TEST_DATABASE_URL)
│   ├── refresher/
│   │   ├── refresher.go        # ticker loop, advisory lock, retention, status
│   │   ├── cache.go            # atomic latest-snapshot cache (+ pre-encoded JSON)
│   │   └── refresher_test.go
│   ├── cli/
│   │   ├── cli.go              # subcommand dispatch: snapshot, migrate, seed, healthcheck, version
│   │   ├── serve.go            # serve, crawl, snapshot --from-db
│   │   └── cli_test.go
│   └── server/
│       ├── server.go           # Fiber app, middleware, routes
│       ├── handlers.go
│       ├── health.go
│       └── handlers_test.go
├── testdata/                   # fixture JSON for the fake HN server
├── Dockerfile
├── docker-compose.yml
├── .dockerignore
├── .env.example
├── Makefile
├── .golangci.yml
├── go.mod / go.sum
├── PLAN.md
└── README.md
```

---

## 4. Output model

`GET /v1/top-stories` returns the latest **completed** snapshot:

```json
{
  "snapshot_id": 1287,
  "status": "completed",
  "started_at": "2026-11-12T17:29:58Z",
  "finished_at": "2026-11-12T17:30:00Z",
  "duration_ms": 1840,
  "stories_requested": 5,
  "skipped_ids": [ { "id": 45990001, "hn_rank": 3, "reason": "job" } ],
  "stats": {
    "items_fetched": 742,
    "authors_fetched": 391,
    "missing_items": 3,
    "failed_items": 0
  },
  "stories": [
    {
      "rank": 1,
      "hn_rank": 1,
      "integrity": {
        "reported_descendants": 120,
        "fetched_comments": 118,
        "ok": 115,
        "deleted": 2,
        "dead": 1,
        "missing": 0,
        "error": 0,
        "max_depth": 9,
        "complete": true
      },
      "missing_items": [
        { "id": 4242, "status": "deleted", "parent_id": 4200, "depth": 3, "path": [1001, 4100, 4200, 4242] }
      ],
      "tree": {
        "id": 1001,
        "type": "story",
        "status": "ok",
        "depth": 0,
        "position": 0,
        "parent_id": null,
        "by": "pg",
        "time": "2026-11-12T15:02:11Z",
        "title": "…",
        "url": "…",
        "score": 512,
        "children": [
          {
            "id": 4100,
            "type": "comment",
            "status": "ok",
            "depth": 1,
            "position": 0,
            "parent_id": 1001,
            "by": "dang",
            "time": "…",
            "text": "…",
            "children": [ /* … */ ]
          }
        ]
      }
    }
  ],
  "authors": {
    "pg":    { "status": "ok", "id": "pg", "created": "2006-10-09T18:21:51Z", "karma": 157000, "about": "…", "submitted_count": 15000 },
    "ghost": { "status": "missing" }
  }
}
```

Design notes:

- **Tree + depth + parent_id + position** make the thread structure explicit. Someone who flattens it, or reads the DB rows, can still rebuild it.
- **`status` on every node** shows gaps *inline*, at the exact position they occur, with any children still attached.
- **`missing_items` per story** is a quick index of every non-`ok` node with its full root-to-node `path`, so nobody has to walk the tree to find gaps.
- **`integrity.complete`** is `true` iff `error == 0 && missing == 0`. Deleted/dead are HN-side states, not fetch gaps.
- **`authors` is deduplicated** into one map keyed by username, so a prolific commenter is fetched and listed once. `submitted` (which can be 10k+ IDs) is reduced to `submitted_count` to keep the payload manageable.
- **Snapshot `status`** is `completed` when every item was fetched, or `partial` when the crawl hit a timeout or had `error` items but still produced data. `running` and `failed` snapshots are never served as "latest".
- `?format=text` renders an indented tree for humans:
  ```
  Snapshot #1287 @ 2026-11-12T17:30:00Z
  [1] #1001 "Show HN: …" by pg (score 512, 120 comments, complete)
    ├─ #4100 dang: "…"
    │   └─ #4200 alice: "…"
    │       └─ #4242 [deleted]
    └─ #4300 [missing]
  ```

---

## 5. Database (PostgreSQL)

### 5.1 Modeling approach

The schema mirrors the output model one-to-one:

| Output | Table |
|---|---|
| Snapshot envelope + `stats` | `snapshots` |
| `stories[]` + `integrity` | `snapshot_stories` |
| Every tree node (story + comments) | `items` (flat adjacency list) |
| `authors{}` | `authors` |
| `missing_items[]` | `missing_items` **view** over `items` |

Everything is **scoped to a snapshot**, so each row records what HN looked like *at crawl time*. Scores, comments and karma change over time, so a global `items` table would mix data from different moments and break the integrity numbers. Snapshot scoping keeps every served response exactly reproducible and gives us history for free. A retention policy keeps the size bounded (see 5.4).

### 5.2 Schema (`migrations/00001_init.sql`)

```sql
-- +goose Up
-- Status columns are TEXT + CHECK instead of enums, so they can be written
-- with plain text parameters and extended without ALTER TYPE.

CREATE TABLE snapshots (
    id                 BIGSERIAL PRIMARY KEY,
    status             TEXT            NOT NULL DEFAULT 'running'
                       CHECK (status IN ('running', 'completed', 'partial', 'failed')),
    started_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    finished_at        TIMESTAMPTZ,
    duration_ms        INTEGER,
    stories_requested  SMALLINT        NOT NULL,
    skipped_ids        JSONB           NOT NULL DEFAULT '[]', -- [{id, hn_rank, reason}]
    items_fetched      INTEGER         NOT NULL DEFAULT 0,
    authors_fetched    INTEGER         NOT NULL DEFAULT 0,
    missing_items      INTEGER         NOT NULL DEFAULT 0,
    failed_items       INTEGER         NOT NULL DEFAULT 0,
    triggered_by       TEXT            NOT NULL CHECK (triggered_by IN ('schedule', 'manual', 'cli', 'seed', 'test')),
    error              TEXT
);
CREATE INDEX snapshots_latest_idx ON snapshots (finished_at DESC)
    WHERE status IN ('completed', 'partial');

CREATE TABLE snapshot_stories (
    snapshot_id           BIGINT   NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    rank                  SMALLINT NOT NULL,           -- our 1..N
    hn_rank               SMALLINT NOT NULL,           -- position in /topstories
    story_id              BIGINT   NOT NULL,
    parts_fetched         INTEGER  NOT NULL DEFAULT 0, -- poll options
    reported_descendants  INTEGER,
    fetched_comments      INTEGER  NOT NULL,
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
    snapshot_id   BIGINT      NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    id            BIGINT      NOT NULL,        -- HN item id
    story_id      BIGINT      NOT NULL,        -- root story this node belongs to
    parent_id     BIGINT,                      -- NULL for the story itself
    depth         SMALLINT    NOT NULL,
    position      INTEGER     NOT NULL,        -- index among siblings (HN `kids`/`parts` order)
    relation      TEXT        NOT NULL DEFAULT 'kid', -- 'root' | 'kid' | 'part' (poll option)
    path          BIGINT[]    NOT NULL,        -- root..self
    status        TEXT        NOT NULL CHECK (status IN ('ok', 'deleted', 'dead', 'missing', 'error')),
    type          TEXT,                        -- 'story' | 'comment' | 'poll' | 'pollopt' | 'job' (NULL if missing/error)
    author        TEXT,                        -- HN `by`
    posted_at     TIMESTAMPTZ,                 -- HN `time`
    title         TEXT,
    url           TEXT,
    text          TEXT,
    score         INTEGER,
    descendants   INTEGER,
    kids_count    INTEGER     NOT NULL DEFAULT 0,
    error_reason  TEXT,                        -- e.g. 'timeout', 'http 503 after 3 retries'
    PRIMARY KEY (snapshot_id, id)
);
CREATE INDEX items_tree_idx   ON items (snapshot_id, story_id, depth, parent_id, position);
CREATE INDEX items_by_idx     ON items (snapshot_id, by);
CREATE INDEX items_gaps_idx   ON items (snapshot_id, story_id) WHERE status <> 'ok';

CREATE TABLE authors (
    snapshot_id      BIGINT      NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    username         TEXT        NOT NULL,
    status           node_status NOT NULL,     -- only ok | missing | error are used
    created          TIMESTAMPTZ,
    karma            INTEGER,
    about            TEXT,
    submitted_count  INTEGER,
    error_reason     TEXT,
    PRIMARY KEY (snapshot_id, username)
);

CREATE VIEW missing_items AS
SELECT snapshot_id, story_id, id, status, parent_id, depth, path, error_reason
FROM items
WHERE status <> 'ok';

-- +goose Down
DROP VIEW  IF EXISTS missing_items;
DROP TABLE IF EXISTS authors;
DROP TABLE IF EXISTS items;
DROP TABLE IF EXISTS snapshot_stories;
DROP TABLE IF EXISTS snapshots;
DROP TYPE  IF EXISTS node_status;
DROP TYPE  IF EXISTS snapshot_status;
```

Notes:

- `path BIGINT[]` gives each `missing_items` entry its root-to-node path with no recursive query, and also lets us answer "whole subtree under X" with `path @> ARRAY[X]` (add a GIN index later if that endpoint is used).
- An item can't appear under two stories in HN, so `(snapshot_id, id)` is a safe primary key. The crawler also keeps a visited set as a guard.
- `author` is not a foreign key to `authors`, because a deleted item has no author and an author fetch can fail independently. The `authors.status` column captures that.

### 5.3 Write path

1. `INSERT INTO snapshots (status='running', triggered_by, stories_requested) RETURNING id`, committed right away so a crash leaves a visible `running` row. On startup, the refresher marks leftover `running` rows as `failed`.
2. The crawler runs and returns an in-memory `model.Snapshot`.
3. A **single transaction** then:
   - `COPY` into `items` and `authors` (`pgx.CopyFrom`, fast even for thousands of rows),
   - `INSERT` the `snapshot_stories` rows,
   - `UPDATE snapshots SET status, finished_at, duration_ms, stats…`.
4. Readers only consider `completed`/`partial` snapshots, so the new snapshot appears **atomically** when the transaction commits.

### 5.4 Read path & retention

- **Latest snapshot**: `SELECT … FROM snapshots WHERE status IN ('completed','partial') ORDER BY finished_at DESC LIMIT 1`.
- **Tree rebuild** (`store/tree.go`): load all `items` for the snapshot ordered by `(story_id, depth, parent_id, position)`, index them by id, attach each to its parent's `children`. This is O(n) and needs no recursive SQL.
- **In-memory cache**: the fully assembled latest `Snapshot` (and its pre-encoded JSON) is cached in process. After a successful save, the refresher swaps the cache, so `GET /v1/top-stories` is a memory read and the DB serves only history queries.
- **Retention**: after each successful crawl, `DELETE FROM snapshots WHERE id NOT IN (latest SNAPSHOT_RETENTION_COUNT)` or older than `SNAPSHOT_RETENTION_AGE`. `ON DELETE CASCADE` cleans up the child rows. Rows with `triggered_by = 'seed'` are **excluded**, because seed data is owned by goose and removed only by `seed down` (§5.6). Monthly partitioning of `items` by snapshot is a possible follow-up if retention ever needs to be long.

### 5.5 Tooling

- Driver: **`github.com/jackc/pgx/v5`** + `pgxpool`.
- Migrations and seeding: **goose** (`github.com/pressly/goose/v3`), see §5.6.
- Queries are hand-written SQL in the `store` package. The surface is small enough that sqlc or an ORM would add more tooling than value.

### 5.6 Migrations & seeding with goose

**Why goose:** each migration is a single SQL file with `-- +goose Up` / `-- +goose Down` sections. It has a programmatic `Provider` API that works with `embed.FS` (no CLI needed at runtime), built-in Postgres session locking, and Go migrations if we ever need them.

**Two goose providers, two version tables:**

| | Migrations (schema) | Seeds (data) |
|---|---|---|
| Directory | `internal/store/migrations/` | `internal/store/seeds/` |
| Version table | `goose_db_version` (default) | `goose_seed_version` |
| Applied by | `serve` on startup (`DB_AUTO_MIGRATE=true`) or `hnfetcher migrate …` | `hnfetcher seed …`, or on startup if `DB_AUTO_SEED=true` (default **false**) |
| Order | Always first | Only after migrations are at the latest version |

Separate version tables let schema and data evolve independently. Seeds can be applied, rolled back and re-applied without touching the schema, and production can simply never run them.

**Wiring (`store/migrate.go`):**

```go
//go:embed migrations/*.sql
var migrationsFS embed.FS

//go:embed seeds/*.sql
var seedsFS embed.FS

func newProvider(db *sql.DB, fsys fs.FS, dir, table string) (*goose.Provider, error) {
    sub, err := fs.Sub(fsys, dir)
    if err != nil { return nil, err }
    locker, err := lock.NewPostgresSessionLocker(lock.WithLockID(lockID)) // distinct id per provider
    if err != nil { return nil, err }
    return goose.NewProvider(goose.DialectPostgres, db, sub,
        goose.WithTableName(table),
        goose.WithSessionLocker(locker), // safe when several replicas start at once
    )
}

func Migrate(ctx context.Context, db *sql.DB) error { /* newProvider(db, migrationsFS, "migrations", "goose_db_version").Up(ctx) */ }
func Seed(ctx context.Context, db *sql.DB) error    { /* newProvider(db, seedsFS, "seeds", "goose_seed_version").Up(ctx) */ }
```

- goose needs a `*sql.DB`, which we get from the existing pool with `stdlib.OpenDBFromPool(pool)` (`github.com/jackc/pgx/v5/stdlib`), so no second driver or connection config is needed.
- Files use **sequential** numbering (`00001_`, `00002_`, …) so the order is obvious in review. New files are created with `make migration name=add_x` / `make seed name=add_y`, which run `go run github.com/pressly/goose/v3/cmd/goose@<pinned> -dir … -s create <name> sql`. Pinning the version avoids needing a global goose install.
- Every migration and seed **must have a working Down**. CI runs `up → down-to 0 → up` for both.
- Multi-statement blocks such as functions or `DO $$ … $$` go inside `-- +goose StatementBegin` / `-- +goose StatementEnd`. Statements that can't run in a transaction (e.g. `CREATE INDEX CONCURRENTLY`) go in their own file with `-- +goose NO TRANSACTION`.

**What the seed contains (`seeds/00001_demo_snapshot.sql`):**

A small, hand-written **demo snapshot** (`triggered_by = 'seed'`) with 2 stories and about 15 items, built so that every case in §1 appears at least once:

- a multi-level thread (depth ≥ 3) with sibling order,
- a `deleted` comment that still has kids,
- a `dead` comment,
- a `missing` item and an `error` item (`error_reason = 'timeout'`),
- a poll with `parts`,
- a `skipped_ids` entry for a job,
- an author with `status = 'missing'`.

Uses:
- `docker compose up` with `DB_AUTO_SEED=true` gives a working API and `format=text` demo **without network access to HN**.
- It's a stable dataset for manual testing of the API and SQL queries.
- It mirrors `testdata/` fixtures, so the crawler test and the seed describe the same scenario.

Seed mechanics:
- Rows are inserted with a CTE (`WITH s AS (INSERT INTO snapshots … RETURNING id) INSERT INTO items SELECT s.id, …`) so no explicit ids are written and `BIGSERIAL` sequences stay correct.
- `finished_at` is set to a fixed past date (`2000-01-01`), so any real crawl immediately becomes the "latest" snapshot.
- Down is `DELETE FROM snapshots WHERE triggered_by = 'seed';`, and cascades remove the rest.

---

## 6. Concurrency design (crawler)

### 6.1 Bounded fetching

All HN requests go through one `hn.Client` that holds a **semaphore** (`golang.org/x/sync/semaphore`) sized by `HN_MAX_CONCURRENCY` (default 32). This caps in-flight I/O no matter how many goroutines are spawned.

### 6.2 Two kinds of errors

| Kind | Examples | Handling |
|---|---|---|
| **Item-level, non-fatal** | One comment times out, a 500 after retries, an author profile fails | **Not returned to the errgroup.** It becomes a node with `status: "error"` and an `error_reason`, or an author with `status: "error"`. The goroutine returns `nil` and the crawl continues. This is what lets the output show which items are missing. |
| **Crawl-level, fatal** | `/topstories` fails, a worker panics, the caller's context is cancelled (shutdown) | A panic is recovered and **returned from `g.Go`**. That cancels the shared context, so every in-flight HN request aborts. `Crawl` returns `(snapshot, err)`. The snapshot is `partial` if any story was fetched and `failed` otherwise, with the reason in `error`. A `/topstories` failure returns `(nil, err)`. |

Hitting `CRAWL_TIMEOUT` is **not** fatal. Unfinished items become `error` nodes with reason `"timeout: …"`, and the snapshot is saved as `partial` with `err == nil`.

### 6.3 Tree walk: nested spawning into one errgroup

Three errgroups per crawl:
- `treeG`: stories and comments. Each node spawns its kids into the same group, and `treeG.Wait()` returns once every depth is done.
- `authG`: author profiles, fanned out.
- A small group per batch while selecting the top N stories.

`authG` is waited **after** `treeG`. Authors are requested only from tree goroutines, which have all finished by then, so no `authG.Go` can race with `authG.Wait`. Both groups derive from one `context.WithCancelCause`, so a panic in either one cancels both.

```go
treeG, tctx := errgroup.WithContext(ctx)
authG, actx := errgroup.WithContext(ctx)
...
for i, kid := range item.Kids {
    slots[i] = placeholder(kid, ..., "not fetched: crawl aborted") // overwritten below
    r.goSafe(treeG, func() {                                       // recover → PanicError + abort
        it, err := r.hn.Item(tctx, kid)                            // semaphore-guarded, retries
        slots[i] = r.build(tctx, treeG, kid, it, err, ...)         // err recorded on node, not returned
    })
}
```

- Each child writes only to **its own slot**, so the tree needs no mutex and keeps HN's order. `Wait()` makes the writes visible to the caller. Tested with `-race`.
- Before `g.Go`, each slot is pre-filled with an `error` placeholder. If the crawl aborts, every unfinished item shows as an explicit gap rather than `nil`.
- **No `g.SetLimit`.** With nested spawning it can deadlock: every slot gets held by a parent that is blocked trying to spawn kids. The semaphore inside `hn.Client` limits real I/O instead.
- errgroup doesn't recover panics. `goSafe` wraps every body, turns a panic into `*PanicError`, logs the stack and cancels the crawl. A panic means a bug, so aborting is the right outcome.
- For `poll` items, `Parts` are walked the same way (`relation: "part"`).
- Each child's `parent` is checked against the id we reached it from. A mismatch is noted in `error_reason`.

### 6.4 Authors

- `authorFetcher` deduplicates under a mutex. The first request claims the name with an `error` placeholder and spawns the fetch into `authG`. Later requests see the name already claimed and return.
- A fetch starts **as soon as** an item reveals its `by`, overlapping with the tree walk.
- A null user gives `{ "status": "missing" }`. A failure gives `{ "status": "error", "error_reason": "…" }`, and it is never returned to `authG`.

### 6.5 Robustness

- Per-request timeout (`HN_REQUEST_TIMEOUT`, default 5s).
- Retries with exponential backoff + jitter on network errors, 429 and 5xx (`HN_MAX_RETRIES`, default 3). A `null` response is **not** retried.
- Crawl timeout (`CRAWL_TIMEOUT`, default 60s). On timeout, unfinished nodes are marked `error` with reason `"timeout"` and the snapshot is stored as `partial`, so we never end up with nothing.
- Depth guard (`MAX_DEPTH`, default 0 = unlimited) and a visited set to guard against cycles. These shouldn't happen on HN, but the check is cheap.
- A shared `http.Client` with a tuned `Transport` (`MaxIdleConnsPerHost` ≈ concurrency) to reuse connections.

---

## 7. Background refresher

- Started by `serve` as a goroutine bound to the app's root context.
- **Runs one crawl right away** at startup, then every `REFRESH_INTERVAL` (default 5m) via `time.Ticker`. If a crawl runs longer than the interval, the next tick waits for it instead of stacking up.
- **Single crawler across replicas**: each run takes `pg_try_advisory_lock(<const>)` on a dedicated connection. If another instance holds the lock, the run is skipped and logged. This lets you scale the API horizontally without duplicate crawls.
- **Manual trigger**: `POST /v1/snapshots/refresh` sends a signal on a buffered channel (capacity 1, so repeated presses collapse into one run) and returns `202 Accepted`. If a crawl is already running, it returns `409`.
- **Failure policy**: if a crawl or save fails, the snapshot row is marked `failed` with `error`, the last good snapshot keeps being served, and the error shows up in `/healthz`.
- Exposes state for health checks: `last_started_at`, `last_finished_at`, `last_status`, `last_error`, `consecutive_failures`, `running`.
- On shutdown it cancels the in-flight crawl. The snapshot row stays `running` and is cleaned up as `failed` on the next startup.

---

## 8. CLI mode

A single binary, `hnfetcher`, with subcommands built on the stdlib `flag` package (no cobra dependency needed for five commands):

| Command | Needs DB | What it does |
|---|---|---|
| `hnfetcher serve` (default) | yes | Runs migrations (if enabled), refresher and HTTP server. |
| `hnfetcher crawl [--limit 5]` | yes | Runs one crawl, saves it as a snapshot (`triggered_by='cli'`), prints the snapshot id and stats. Handy for cron or seeding. |
| `hnfetcher snapshot [--limit 5] [--format json\|text] [--out file]` | no | Crawls and writes the output to stdout or a file **without touching Postgres**. Good for submitting "the output" as a file. |
| `hnfetcher snapshot --from-db [--id N]` | yes | Prints a stored snapshot (latest by default) in the same format. |
| `hnfetcher migrate up\|down\|down-to N\|redo\|status\|version` | yes | Runs the goose **schema** provider over the embedded migrations. |
| `hnfetcher seed up\|down\|reset\|status` | yes | Runs the goose **seed** provider (`goose_seed_version`). Refuses to run if the schema isn't at the latest version. |
| `hnfetcher healthcheck` | no | Calls `http://127.0.0.1:$PORT/livez` and exits 0/1. Used by Docker. |

All commands read the same env config, and flags override env.

---

## 9. HTTP API (Fiber v3)

Framework: **`github.com/gofiber/fiber/v3`**.

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/top-stories` | Latest completed/partial snapshot (served from memory). Query: `limit` (≤ stories in snapshot), `format` (`json`\|`text`). `503` if no snapshot exists yet. |
| `GET` | `/v1/snapshots` | Paginated list of snapshot envelopes (`id`, `status`, timestamps, stats). Query: `limit`, `before_id`, `status`. |
| `GET` | `/v1/snapshots/:id` | Full stored snapshot in the same shape as `/v1/top-stories`. Supports `format`. |
| `GET` | `/v1/snapshots/:id/stories/:story_id` | One story's tree, integrity and missing items, plus only the authors it involves. |
| `GET` | `/v1/authors/:username` | Author profile from the latest snapshot, plus the item ids they wrote in that snapshot. |
| `POST` | `/v1/snapshots/refresh` | Triggers a background crawl. `202` / `409`. |
| `GET` | `/livez` | Liveness: process is up. Always `200` unless shutting down. |
| `GET` | `/readyz` | Readiness: Postgres reachable **and** at least one usable snapshot exists. `503` otherwise. |
| `GET` | `/healthz` | Detailed, human-friendly JSON (see below). |

`/healthz` example:

```json
{
  "status": "degraded",
  "version": "1.0.0",
  "uptime_s": 3600,
  "checks": {
    "postgres": { "status": "up", "latency_ms": 2 },
    "hn_api":   { "status": "up", "latency_ms": 140 },
    "refresher": {
      "status": "failing",
      "running": false,
      "last_status": "failed",
      "last_finished_at": "2026-11-12T17:30:00Z",
      "last_error": "context deadline exceeded",
      "consecutive_failures": 2,
      "latest_snapshot_id": 1287,
      "latest_snapshot_age_s": 610
    }
  }
}
```

- `hn_api` is checked with `GET /maxitem.json`, and the result is cached for about 10s so probes don't spam HN. HN being down makes the service **degraded**, not unready, because we can still serve the last snapshot.
- Overall `status` is `ok`, `degraded` (HN down, refresher failing, or snapshot older than `3 × REFRESH_INTERVAL`) or `down` (no DB or no snapshot).

Middleware: `recover`, `requestid`, `logger` (structured, via `log/slog`), Fiber's built-in **`healthcheck`** middleware for `/livez` and `/readyz` with custom probe functions, `limiter` on `POST /v1/snapshots/refresh`, and `compress` (snapshots are a few hundred KB of JSON).

Error responses all use one shape: `{ "error": { "code": "...", "message": "..." } }`.

Graceful shutdown: trap `SIGINT`/`SIGTERM`, make `/livez` and `/readyz` fail, stop the refresher, `app.ShutdownWithTimeout(10s)`, then close the pg pool.

---

## 10. Configuration (env vars)

| Var | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | HTTP port |
| `DATABASE_URL` | — (required for `serve`/`crawl`) | e.g. `postgres://hn:hn@localhost:5432/hn?sslmode=disable` |
| `DB_MAX_CONNS` | `10` | pgxpool max connections |
| `DB_AUTO_MIGRATE` | `true` | Run goose migrations on `serve` startup |
| `DB_AUTO_SEED` | `false` | Apply goose seeds on `serve` startup (after migrations). Dev/demo only |
| `HN_BASE_URL` | `https://hacker-news.firebaseio.com/v0` | Overridable for tests |
| `HN_MAX_CONCURRENCY` | `32` | Max in-flight HN requests |
| `HN_REQUEST_TIMEOUT` | `5s` | Per request |
| `HN_MAX_RETRIES` | `3` | Retries on transient errors |
| `TOP_STORIES` | `5` | Stories per crawl |
| `TOP_STORIES_INCLUDE_JOBS` | `false` | If `false`, skip `job` items in `/topstories` (§2) |
| `CRAWL_TIMEOUT` | `60s` | Whole crawl |
| `MAX_DEPTH` | `0` | 0 = unlimited |
| `REFRESH_INTERVAL` | `5m` | Background crawl interval |
| `REFRESH_ENABLED` | `true` | Turn off to run as a read-only API replica |
| `SNAPSHOT_RETENTION_COUNT` | `288` | Keep the last N snapshots (≈ 1 day at 5m) |
| `SNAPSHOT_RETENTION_AGE` | `168h` | And/or drop snapshots older than this |
| `LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` |

---

## 11. Docker & docker-compose

### Dockerfile (multi-stage)

```dockerfile
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X github.com/luk3skyw4lker/hackernews-fetcher/internal/cli.Version=${VERSION}" -o /out/hnfetcher ./cmd/hnfetcher

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hnfetcher /hnfetcher
EXPOSE 8080
USER nonroot:nonroot
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 CMD ["/hnfetcher", "healthcheck"]
ENTRYPOINT ["/hnfetcher"]
CMD ["serve"]
```

Distroless has no `curl`/`wget`, so the binary's own `healthcheck` subcommand does the probing. Migrations are embedded in the binary, so no extra files need copying.

### docker-compose.yml

```yaml
services:
  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: hn
      POSTGRES_PASSWORD: hn
      POSTGRES_DB: hn
    ports:
      - "${POSTGRES_PORT:-5432}:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U hn -d hn"]
      interval: 5s
      timeout: 3s
      retries: 10
    restart: unless-stopped

  hn-fetcher:
    build: .
    image: hn-fetcher:local
    depends_on:
      postgres:
        condition: service_healthy
    ports:
      - "${PORT:-8080}:8080"
    environment:
      PORT: 8080
      DATABASE_URL: postgres://hn:hn@postgres:5432/hn?sslmode=disable
      DB_AUTO_MIGRATE: "true"
      DB_AUTO_SEED: "${DB_AUTO_SEED:-false}"   # set true for an offline demo dataset
      REFRESH_INTERVAL: 5m
      HN_MAX_CONCURRENCY: 32
      LOG_LEVEL: info
    healthcheck:
      test: ["CMD", "/hnfetcher", "healthcheck"]
      interval: 15s
      timeout: 3s
      retries: 3
      start_period: 10s
    restart: unless-stopped

volumes:
  pgdata:
```

One-off CLI runs through compose:
`docker compose run --rm hn-fetcher snapshot --format text`, `docker compose run --rm hn-fetcher crawl`, `docker compose run --rm hn-fetcher migrate status`, `docker compose run --rm hn-fetcher seed up`.

`.env.example` documents the overridable ports and settings.

---

## 12. Testing strategy

- **Fake HN server** (`internal/hntest`, an `httptest.Server`) backed by `testdata/hn_fixture.json` and injected via `HN_BASE_URL`. The fixture includes:
  - a normal multi-level thread,
  - a deleted comment that still has kids,
  - a dead comment,
  - a kid ID that returns `null` (missing),
  - an ID that returns 500 every time (error after retries),
  - an author whose profile is `null`,
  - a `job` in the top list (skipped and recorded in `skipped_ids`),
  - a `poll` with `parts` (options fetched and their authors included),
  - an item with extra, unknown JSON fields (ignored without error).

  Per-test hooks (`Delay`, `FailTimes`, `FailAlways`) inject latency and transient or permanent failures on any path, covering crawl timeouts (`partial`) and retries.
- **Store tests** use testcontainers-go (`TEST_PG_IMAGE` overrides the image) or an existing server via `TEST_DATABASE_URL`; each test gets its own database. They skip if neither is available.
- **Crawler tests**: check tree shape, depth, position/order, statuses, `missing_items` paths, integrity counts, and author dedup (each user requested exactly once, counted by the fake server).
- **Concurrency tests**: run with `-race`, and check that the fake server never sees more than `HN_MAX_CONCURRENCY` requests in flight.
- **Client tests**: retry/backoff on 5xx/429, no retry on null, timeout handling.
- **Store tests** (`testcontainers-go` Postgres module):
  - **Round-trip**: crawl the fixtures, save, load, then `cmp.Diff` against the in-memory snapshot. This is the key test proving the DB model matches the output.
  - Running snapshots are invisible to readers; a failed tx leaves no partial rows.
  - Retention deletes old snapshots and cascades.
  - goose migrations `up → down-to 0 → up` with no errors, and `status` reports no pending files.
  - goose seeds `up → down → up`. After `up`, the seed snapshot loads and contains every status. After `down`, no `triggered_by='seed'` rows remain.
  - Retention never deletes seed snapshots.
  - Seed provider refuses to run when the schema isn't at the latest version.
- **Refresher tests**: advisory lock skips concurrent runs, manual triggers collapse, failure keeps the last good snapshot, stale `running` rows become `failed` on startup.
- **Handler tests**: `app.Test(req)` for every endpoint, `limit`/`format` validation, `503` before the first snapshot, health endpoints in the ok/degraded/down states.
- **CLI tests**: `snapshot` writes valid JSON/text without a DB.
- Optional **integration test** behind a build tag (`//go:build integration`) that hits the real HN API.

---

## 13. Implementation steps (in order)

1. **Scaffold**: layout above; `go get github.com/gofiber/fiber/v3 github.com/jackc/pgx/v5 github.com/pressly/goose/v3 golang.org/x/sync`; Makefile (`run`, `test`, `test-integration`, `lint`, `build`, `docker-up`, `docker-down`, `migrate-up`, `migrate-down`, `migrate-status`, `migration name=…`, `seed-up`, `seed-down`, `seed name=…`).
2. **Config**: env parsing with defaults and validation.
3. **`hn.Client`**: models, `TopStories`, `Item`, `User`, `MaxItem`, semaphore, retries, timeouts. Tests with the fake server.
4. **`model` package**: `Snapshot`, `Story`, `Node`, `Author`, `Integrity`, `MissingItem`, status enums, text renderer.
5. **`authorFetcher`**: mutex dedup, own errgroup (`authG`), panic recovery. Tests.
6. **`Crawler`**: concurrent walk, integrity computation, missing-item index, timeout → `partial`. Tests (incl. `-race`).
7. **CLI `snapshot`** (no DB): first end-to-end check against the real HN API.
8. **Store**: goose schema migration `00001_init.sql`, migration/seed providers, writer (COPY in one tx), reader + tree rebuild, retention. Round-trip tests with testcontainers.
9. **Seeds**: `seeds/00001_demo_snapshot.sql` mirroring `testdata/`. Seed tests.
10. **CLI `migrate`, `seed` and `crawl`**.
11. **Refresher**: ticker loop, advisory lock, manual trigger, startup cleanup, status reporting, in-memory latest cache swap.
12. **Fiber server**: routes, middleware, error handler, `format=text`.
13. **Health**: `/livez`, `/readyz`, `/healthz`; `healthcheck` subcommand; graceful shutdown.
14. **Docker**: Dockerfile, `.dockerignore`, `docker-compose.yml`, `.env.example`; verify `docker compose up` reports both services `healthy` and that a snapshot appears.
15. **README** (see §14).
16. **Polish**: `golangci-lint`, a final manual run, and a check that both the API output and the DB rows answer all three questions in §1.

---

## 14. README outline

The `README.md` will contain:

1. **Overview**: what the service does, the three questions its output answers, and an architecture diagram.
2. **Quick start**
   - Docker: `docker compose up --build`, wait about 10s for the first crawl, then `curl localhost:8080/v1/top-stories | jq`.
   - Local: start Postgres (`docker compose up -d postgres`), `export DATABASE_URL=…`, `go run ./cmd/hnfetcher serve` (or `make run`).
   - No DB: `go run ./cmd/hnfetcher snapshot --format text`.
3. **Requirements**: Go 1.26+, Docker 24+ / Compose v2, Postgres 17 (provided by compose), internet access to `hacker-news.firebaseio.com`.
4. **Configuration**: the env var table from §10.
5. **CLI reference**: every subcommand with examples (local and via `docker compose run`).
6. **API reference**: endpoints, query params, example requests/responses, `format=text` example.
7. **Reading the output**: how to follow a thread (`depth`, `parent_id`, `position`, `children`), where authors live (`by` + `authors` map), how to spot gaps (table of `status` values, `missing_items`, `integrity.complete`, snapshot `partial`).
8. **Database**: ER diagram, table descriptions, why data is snapshot-scoped, retention, goose migrations vs seeds (separate version tables, how to apply/roll back each), the demo seed dataset, and handy SQL queries (latest snapshot, gaps per story, top commenters, a story's thread via `path`).
9. **Background refresh**: interval, manual trigger, multi-replica safety via advisory lock, failure behavior.
10. **Health checks**: `/livez`, `/readyz`, `/healthz` meanings, ok/degraded/down, and how Docker/Compose use them.
11. **Concurrency & resilience**: bounded semaphore, author dedup, retries, timeouts, partial snapshots.
12. **Development**: `make test` (with `-race`; needs Docker for testcontainers), `make lint`, fixtures, integration tests, creating a migration or seed with `make migration name=…` / `make seed name=…`, the "every file needs a working Down" rule.
13. **Project structure**: the tree from §3.
14. **Design decisions & trade-offs**: snapshot-scoped vs global tables, why deleted/dead don't mark a story incomplete, why `submitted` is reduced to a count, why errors don't cancel the crawl, why HN being down is "degraded" rather than "unready".

---

## 15. Decisions log

| Topic | Decision |
|---|---|
| Framework | **Fiber v3**, using its built-in `healthcheck` middleware. |
| Data freshness | **Background refresh** on an interval plus a manual trigger. The API always serves from Postgres (latest snapshot cached in memory). |
| Storage | **PostgreSQL**, schema mirroring the output, scoped per snapshot, with retention. |
| Migrations & seeding | **goose** (`pressly/goose/v3`), embedded SQL files. Two providers: schema (`goose_db_version`) and seeds (`goose_seed_version`). Seeds are opt-in (`DB_AUTO_SEED`). |
| CLI | **Yes**: `serve`, `crawl`, `snapshot` (with or without DB), `migrate`, `seed`, `healthcheck` in one binary. |
| Jobs in `/topstories` | Skipped by default and recorded in `skipped_ids`. Configurable. |
| Polls | `parts` are fetched as a separate `parts` list on the poll node, not mixed into the comment tree. |
| HTML fields | Stored and returned raw. Stripped only for `format=text`. |
| Transport | REST polling over Firebase streaming, which fits the snapshot model. |

### Still open (small, can decide during implementation)

- Retention defaults (288 snapshots / 7 days) are guesses. Adjust to how much history we actually want.
- Future optimization: use `/v0/updates.json` to re-fetch only changed items between snapshots instead of a full re-crawl. Not worth the complexity for 5 stories, and full crawls keep each snapshot self-consistent.
- Whether to also store each snapshot's rendered JSON in a `jsonb` column for zero-cost historical reads. Not needed initially, because the tree rebuild is O(n) and fast.
