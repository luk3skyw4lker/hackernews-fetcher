# hackernews-fetcher

A Go service that fetches the **top 5 HackerNews stories with their full comment trees and every author's profile**, concurrently, stores each crawl as a snapshot in PostgreSQL and serves it over a small HTTP API (Fiber v3). The same binary also works as a CLI.

The output alone answers three questions:

1. **What is the thread structure?** Every node has `depth`, `parent_id`, `position` and nested `children`, plus a root-to-node `path`.
2. **Who wrote each post and comment?** Every node has `by`, and the snapshot has an `authors` map with each user's profile, fetched once per crawl.
3. **Is anything missing?** Every node has a `status` (`ok`, `deleted`, `dead`, `missing`, `error`). Each story has an `integrity` summary and a `missing_items` index of every gap, and the snapshot is marked `completed` or `partial`.

```
                 ┌────────────────────────────────────────┐
  HTTP ─────────▶│ Fiber v3 app (handlers, middleware)    │
                 └───────────────┬────────────────────────┘
                                 │ reads (latest snapshot cached in memory)
                        ┌────────▼────────┐
                        │   PostgreSQL    │
                        └────────▲────────┘
                                 │ writes (one tx per snapshot)
                 ┌───────────────┴────────────────────────┐
  ticker ───────▶│ refresher                              │◀── POST /v1/snapshots/refresh
  CLI `crawl` ──▶│  advisory lock: one crawl at a time    │
                 └───────────────┬────────────────────────┘
                 ┌───────────────▼────────────────────────┐
                 │ crawler: concurrent tree walk,         │
                 │ deduplicated author fetch              │
                 └───────────────┬────────────────────────┘
                 ┌───────────────▼────────────────────────┐
                 │ hn.Client: semaphore, retries, timeout │
                 └────────────────────────────────────────┘
```

---

## Quick start

**Docker (everything):**

```sh
docker compose up --build
# the first crawl takes ~15-40s; until then /v1/top-stories returns 503
curl -s localhost:8080/v1/top-stories | jq '.stats'
curl -s 'localhost:8080/v1/top-stories?format=text' | less
```

Set `DB_AUTO_SEED=true` (`DB_AUTO_SEED=true docker compose up --build`) to load a small offline demo snapshot, which is served right away until the first live crawl finishes.

**Local Go, Postgres from compose:**

```sh
docker compose up -d postgres
export DATABASE_URL='postgres://hn:hn@localhost:5432/hn?sslmode=disable'
go run ./cmd/hnfetcher serve      # or: make run
```

**No database at all:** crawl once and print the result.

```sh
go run ./cmd/hnfetcher snapshot --format text
go run ./cmd/hnfetcher snapshot --out snapshot.json
```

## Requirements

- Go 1.26+
- Docker 24+ with Compose v2 (for the stack and for the store tests)
- PostgreSQL 17 (provided by compose; 14+ works)
- Internet access to `hacker-news.firebaseio.com`

## Configuration

Everything is configured through environment variables (see `.env.example`). CLI flags override them where available.

| Var | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | HTTP port |
| `DATABASE_URL` | (required for `serve`, `crawl`, `migrate`, `seed`, `snapshot --from-db`) | e.g. `postgres://hn:hn@localhost:5432/hn?sslmode=disable` |
| `DB_MAX_CONNS` | `10` | pgxpool max connections |
| `DB_AUTO_MIGRATE` | `true` | Run schema migrations on `serve`/`crawl` startup |
| `DB_AUTO_SEED` | `false` | Apply the demo seed on `serve` startup (dev/demo only) |
| `HN_BASE_URL` | `https://hacker-news.firebaseio.com/v0` | Overridable for tests |
| `HN_MAX_CONCURRENCY` | `32` | Max in-flight HN requests |
| `HN_REQUEST_TIMEOUT` | `5s` | Per request |
| `HN_MAX_RETRIES` | `3` | Retries on network errors, 5xx and 429 |
| `TOP_STORIES` | `5` | Stories per crawl (1-100) |
| `TOP_STORIES_INCLUDE_JOBS` | `false` | If `false`, `job` posts in `/topstories` are skipped and listed in `skipped_ids` |
| `CRAWL_TIMEOUT` | `60s` | Whole crawl; on timeout the snapshot is stored as `partial` |
| `MAX_DEPTH` | `0` | Comment depth limit, 0 = unlimited |
| `REFRESH_INTERVAL` | `5m` | Background crawl interval |
| `REFRESH_ENABLED` | `true` | `false` = read-only replica that only reloads the latest snapshot |
| `SNAPSHOT_RETENTION_COUNT` | `288` | Keep the newest N snapshots (≈ 1 day at 5m), 0 = no limit |
| `SNAPSHOT_RETENTION_AGE` | `168h` | Also drop snapshots older than this, 0 = no limit |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

## CLI reference

One binary, `hnfetcher`:

| Command | Needs DB | What it does |
|---|---|---|
| `serve` (default) | yes | Migrations (if enabled), background refresher and HTTP API. |
| `crawl [--limit N]` | yes | One crawl, stored as a snapshot with `trigger=cli`. Prints the id and stats. Fails fast if another crawl holds the lock. |
| `snapshot [--limit N] [--format json\|text] [--out FILE]` | no | Crawl and print, **without touching Postgres**. |
| `snapshot --from-db [--id N] [--format …] [--limit N]` | yes | Print a stored snapshot (latest by default). |
| `migrate up\|down\|down-to N\|redo\|status\|version` | yes | goose schema migrations (`goose_db_version`). |
| `seed up\|down\|reset\|status` | yes | goose seeds (`goose_seed_version`). Refuses to run on an outdated schema. |
| `healthcheck` | no | GET `http://127.0.0.1:$PORT/livez`, exit 0/1 (used by Docker). |
| `version` | no | Print the build version. |

Exit codes: `0` ok, `1` error, `2` usage error. If a crawl is aborted (for example by Ctrl-C), `snapshot` still writes what it got and then exits `1`.

Through compose:

```sh
docker compose run --rm hn-fetcher snapshot --format text
docker compose run --rm hn-fetcher crawl
docker compose run --rm hn-fetcher migrate status
docker compose run --rm hn-fetcher seed up
```

## API reference

All errors use one shape: `{"error": {"code": "bad_request", "message": "limit must be an integer between 1 and 5"}}`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/top-stories` | Latest completed or partial snapshot, served from memory. Query: `limit` (1 to the number of stories), `format` (`json`, `text`). `503 no_snapshot` before the first snapshot exists. |
| `GET` | `/v1/snapshots` | Snapshot envelopes, newest first. Query: `limit` (1-100, default 20), `before_id` (pagination), `status`. The response includes `next_before_id` when there may be more. |
| `GET` | `/v1/snapshots/:id` | One stored snapshot in the same shape as `/v1/top-stories`. Supports `limit` and `format`. |
| `GET` | `/v1/snapshots/:id/stories/:story_id` | One story (HN id): tree, integrity, missing items, plus only the authors it involves. |
| `GET` | `/v1/authors/:username` | Profile from the latest snapshot plus `item_ids` they wrote in it. |
| `POST` | `/v1/snapshots/refresh` | Schedule a crawl now. `202` accepted, `409 crawl_running`, `503 refresh_disabled`, `429` past 5 per minute per client. |
| `GET` | `/livez`, `/readyz`, `/healthz` | See [Health checks](#health-checks). |

Responses are gzip/brotli compressed when the client accepts it. A 5-story snapshot is about 700 KB of JSON, or 180 KB gzipped.

```sh
curl -s localhost:8080/v1/top-stories?limit=1 | jq '.stories[0] | {integrity, missing_items}'
curl -s 'localhost:8080/v1/snapshots?limit=5' | jq '.snapshots[] | {snapshot_id, status, stats}'
curl -s localhost:8080/v1/snapshots/12/stories/50492601 | jq '.story.tree.children[0]'
curl -s localhost:8080/v1/authors/dang | jq
curl -s -X POST localhost:8080/v1/snapshots/refresh
```

`format=text` (an excerpt from a live crawl):

```
Snapshot #2 @ 2026-11-14T02:33:29Z [completed] — 1160 items, 716 authors, 0 missing, 0 failed

[1] #50492601 "FAA lifted National Ground Stop at 0119Z" by d3nit (story, score 126, HN rank 1)
    comments: HN reports 104, fetched 115 (ok 104, deleted 7, dead 4, missing 0, error 0), max depth 7 — complete
    ├─ #50492971 elicash: Here's what they said caused it in the White House briefing: …
    │  ├─ #50493089 drehm: Duffy also kept saying they were working as quickly as they possibly could …
    │  ├─ #50493024 [deleted]
    │  └─ #50493065 hn_throwaway_99: I can't tell if you're being ironic or not. …
    ├─ #50492807 souvlakee: The reason field is empty, but the website footer provides enough information …
    │  └─ #50492835 g-b-r: And yet, it seems to have been a careless update to have broken everything
    │     └─ #50492862 cozzyd: probably updating winzip
    │        └─ #50492874 g-b-r [dead]: [flagged]
```

## Reading the output

```jsonc
{
  "snapshot_id": 2, "status": "completed", "trigger": "schedule",
  "started_at": "…", "finished_at": "…", "duration_ms": 15340, "stories_requested": 5,
  "skipped_ids": [{"id": 900, "hn_rank": 2, "reason": "job"}],
  "stats": {"items_fetched": 1160, "authors_fetched": 716, "missing_items": 0, "failed_items": 0},
  "stories": [{
    "rank": 1, "hn_rank": 1,
    "integrity": {"reported_descendants": 104, "fetched_comments": 115, "parts_fetched": 0,
                  "ok": 104, "deleted": 7, "dead": 4, "missing": 0, "error": 0, "max_depth": 7, "complete": true},
    "missing_items": [{"id": 50493024, "status": "deleted", "relation": "kid", "parent_id": 50492971, "depth": 2,
                       "path": [50492601, 50492971, 50493024]}],
    "tree": {"id": 50492601, "type": "story", "status": "ok", "relation": "root", "depth": 0, "position": 0,
             "parent_id": null, "path": [50492601], "by": "d3nit", "time": "…", "title": "…", "score": 126,
             "children": [{"id": 50492971, "type": "comment", "depth": 1, "parent_id": 50492601, "by": "elicash", "children": ["…"]}]}
  }],
  "authors": {"elicash": {"id": "elicash", "status": "ok", "created": "…", "karma": 1234, "submitted_count": 567},
              "ghost":   {"id": "ghost", "status": "missing"}}
}
```

- **Following a thread:** start at `tree` and walk `children` in order. `position` is the index in HN's `kids` order (HN's ranking), `depth` is 0 for the story, and `parent_id` plus `path` let you rebuild the tree from flat rows. Poll options are under `parts`, not `children`.
- **Authors:** every node's `by` is a key in the top-level `authors` map. Profiles are fetched once per crawl however many times someone comments. `submitted` is reduced to `submitted_count`.
- **Gaps:**

  | `status` | Meaning | Counts against `complete`? |
  |---|---|---|
  | `ok` | Fetched with content | no |
  | `deleted` | HN says deleted (its replies are still fetched and attached) | no, it's HN-side |
  | `dead` | HN says dead (flagged/killed) | no, it's HN-side |
  | `missing` | HN returned `null` for the id | **yes** |
  | `error` | We couldn't fetch it: network or 5xx after retries, crawl timeout, depth limit (`error_reason` says which) | **yes** |

  `integrity.complete` is `true` when `missing == 0 && error == 0`. `missing_items` lists every non-`ok` node with its path, so you don't have to walk the tree. The snapshot is `partial` when anything errored or the crawl timed out, in which case `error` holds the reason. `fetched_comments` can be larger than HN's `descendants`, because HN doesn't count deleted and dead comments.

## Database

```
snapshots 1───* snapshot_stories        (rank, story_id, integrity counts)
          1───* items                   (one row per node: tree position, status, content)
          1───* authors                 (one row per username)
view missing_items = items WHERE status <> 'ok'
```

| Table | Contents |
|---|---|
| `snapshots` | One row per crawl: status (`running`, `completed`, `partial`, `failed`), trigger, timings, stats, `skipped_ids` (jsonb), error. |
| `snapshot_stories` | The ranked stories of a snapshot with their integrity summary. |
| `items` | Every story, comment and poll option as observed: `parent_id`, `depth`, `position`, `relation` (`root`/`kid`/`part`), `path BIGINT[]`, `status`, `author`, `posted_at`, content, `error_reason`. |
| `authors` | Profile per username per snapshot (`ok`, `missing`, `error`). |

**Why snapshot-scoped rows:** HN content changes constantly (scores, new replies, deletions). Scoping every row to a snapshot keeps each crawl self-consistent and exactly reproducible: the API reads the same rows the crawler wrote, and a round-trip test proves `load(save(snapshot)) == snapshot`. The cost is duplicate data across snapshots, which is why there's retention.

**Writes:** a snapshot row is created as `running`, then everything is written in **one transaction** (COPY into `items`, `authors` and `snapshot_stories`, then the envelope update). Readers only ever see `completed` and `partial` snapshots. Rows left `running` by a crash become `failed` on the next startup.

**Retention:** after each crawl, snapshots beyond `SNAPSHOT_RETENTION_COUNT` or older than `SNAPSHOT_RETENTION_AGE` are deleted (children cascade). Seed snapshots are never deleted by retention.

**Migrations vs seeds (goose):** both are embedded in the binary and run by two separate goose providers with their own version tables and advisory locks:

- `internal/store/migrations/`: schema, tracked in `goose_db_version`. Applied automatically by `serve` (`DB_AUTO_MIGRATE`), or with `hnfetcher migrate up|down|down-to N|redo|status|version`.
- `internal/store/seeds/`: data, tracked in `goose_seed_version`. Applied with `DB_AUTO_SEED=true` or `hnfetcher seed up|down|reset|status`. The demo seed mirrors `testdata/hn_fixture.json`: 2 stories, a skipped job, deleted, dead, missing and error comments, a poll with options, and a missing author. Its `finished_at` is in the year 2000, so any real crawl takes over as "latest". `seed down` removes it.

Handy queries:

```sql
-- latest servable snapshot
SELECT id, status, finished_at, items_fetched FROM snapshots
WHERE status IN ('completed','partial') ORDER BY finished_at DESC LIMIT 1;

-- gaps per story in a snapshot
SELECT story_id, status, count(*) FROM missing_items WHERE snapshot_id = 2 GROUP BY 1, 2;

-- top commenters in a snapshot
SELECT author, count(*) FROM items WHERE snapshot_id = 2 AND relation = 'kid' AND author IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- a story's thread in display order, indented
SELECT repeat('  ', depth) || COALESCE(author, '[' || status || ']') AS who, id
FROM items WHERE snapshot_id = 2 AND story_id = 50492601 ORDER BY path;
```

(`ORDER BY path` gives depth-first order, though siblings come out in id order rather than HN's ranking. Use `position` for HN's order.)

## Background refresh

- `serve` loads the latest stored snapshot into memory, crawls immediately, then crawls every `REFRESH_INTERVAL`. A crawl that runs longer than the interval delays the next tick instead of stacking runs.
- **One crawl at a time across replicas:** every crawl (scheduled, manual or `hnfetcher crawl`) holds a Postgres advisory lock. A replica that can't get the lock skips its turn and reloads whatever the other replica stored, so the API scales horizontally without duplicate crawls. `REFRESH_ENABLED=false` makes a pure read replica.
- **Manual trigger:** `POST /v1/snapshots/refresh`. Repeated presses collapse into one run; `409` if a crawl is already running.
- **Failure:** if a crawl or save fails, its row is marked `failed` with the error, the last good snapshot keeps being served, and `/healthz` reports the refresher as `failing` with `consecutive_failures`.
- **Shutdown:** the in-flight crawl is canceled. Whatever it fetched is still saved as `partial`.

## Health checks

| Endpoint | Meaning | Used by |
|---|---|---|
| `/livez` | The process is up. `200`, or `503` while shutting down. | Docker `HEALTHCHECK` (via `hnfetcher healthcheck`), k8s liveness |
| `/readyz` | Postgres answers **and** a snapshot is available to serve. HN being down does **not** make the service unready. | load balancers, k8s readiness |
| `/healthz` | Detailed JSON for humans and dashboards. `200` for ok/degraded, `503` for down. | monitoring |

```json
{"status": "ok", "version": "v1.0.0", "uptime_s": 15,
 "checks": {"postgres": {"status": "up", "latency_ms": 1},
            "hn_api":   {"status": "up", "latency_ms": 389},
            "refresher": {"status": "ok", "enabled": true, "running": false, "last_status": "completed",
                          "last_finished_at": "…", "consecutive_failures": 0,
                          "latest_snapshot_id": 2, "latest_snapshot_age_s": 0}}}
```

`status` is `down` (no DB or no snapshot), `degraded` (HN unreachable, refresher failing, or the snapshot is older than 3 × `REFRESH_INTERVAL`) or `ok`. The HN probe (`/maxitem.json`) is cached for 10 seconds, so frequent probes don't become HN traffic.

## Concurrency & resilience

- **Bounded fetching:** all HN requests go through one semaphore (`HN_MAX_CONCURRENCY`) inside `hn.Client`, with per-request timeouts and retries with exponential backoff and jitter on network errors, 5xx and 429. `null` responses are never retried; they mean `missing`.
- **Nested spawning:** each fetched node spawns its children into one `errgroup`, so the whole tree is walked concurrently. `errgroup.SetLimit` isn't used, because parents waiting to spawn children could deadlock on it. The semaphore bounds the work instead.
- **Two kinds of errors:** an *item* that fails is recorded on its node (`status: error` with `error_reason`) and never cancels the crawl. Only *crawl-level* failures abort: a worker panic (recovered and reported as an error), the caller canceling, or `/topstories` failing. A crawl timeout keeps everything fetched so far and returns a `partial` snapshot.
- **Author dedup:** authors are fetched in their own errgroup as soon as a node reveals them. A mutex-guarded map makes sure each username is requested once per crawl. A live 5-story crawl makes about 1,200 item requests and 700 user requests and takes about 15-35 seconds.
- **Placeholders first:** every child slot is written as a placeholder before its goroutine starts, so an aborted crawl still shows exactly what wasn't fetched.

## Development

```sh
make test               # go test -race ./... (store tests start Postgres via testcontainers)
make lint               # go vet + golangci-lint
make test-integration   # adds a test against the real HN API
make snapshot           # crawl once, print text
make migration name=add_index   # new goose schema file
make seed name=more_demo        # new goose seed file
```

- Store tests need Docker (testcontainers-go). To use an existing server instead, set `TEST_DATABASE_URL=postgres://…/postgres`: each test creates and drops its own database. If neither is available, the store tests are skipped with a message. `TEST_PG_IMAGE` overrides the container image.
- Crawler, server and CLI tests use a fake HN server (`internal/hntest`) backed by `testdata/hn_fixture.json`, which covers deleted (with replies), dead, missing, error (HTTP 500), job, poll and unknown-field items plus a missing author. Tests inject latency and failures per path to exercise timeouts, retries and panics.
- **Every migration and seed needs a working `Down`.** The tests run `up → down-to 0 → up` for the schema and `up → down → up` for seeds.

## Project structure

```
cmd/hnfetcher/            main: signal handling, exit codes
internal/
  cli/                    subcommands (serve, crawl, snapshot, migrate, seed, healthcheck)
  config/                 env config + validation
  hn/                     HN API client (semaphore, retries, timeouts)
  hntest/                 fake HN server for tests
  crawler/                concurrent tree walk, author fetcher, integrity
  model/                  output contract, summaries, text renderer
  store/                  pgx store: writer (COPY), reader + tree rebuild, retention,
                          advisory lock, goose migrations/ and seeds/
  refresher/              ticker loop, manual trigger, latest-snapshot cache
  server/                 Fiber app, handlers, health
testdata/                 HN fixture
Dockerfile, docker-compose.yml, Makefile, .env.example
```

## Design decisions & trade-offs

- **Snapshot-scoped tables instead of global `items`/`users` tables.** Each crawl is a consistent picture, the DB matches the API one-to-one, and history is easy. The cost is duplicate storage, bounded by retention.
- **Deleted and dead don't make a story incomplete.** They're HN-side states, and the data we have is accurate. Only `missing` and `error` mean we failed to observe something.
- **`submitted` becomes a count.** Prolific users have tens of thousands of ids, which would bloat every snapshot without helping answer any of the three questions.
- **Item errors don't cancel the crawl.** One flaky comment shouldn't throw away 1,000 good ones. It's recorded in place, and the snapshot becomes `partial`.
- **HN down is "degraded", not "unready".** We can still serve the last snapshot, so taking the service out of rotation would only make things worse.
- **TEXT + CHECK instead of Postgres enums**, so COPY works without registering custom types on every connection.
- **Polling the REST API instead of Firebase streaming.** Snapshots are periodic by nature, and each one stays self-consistent.
