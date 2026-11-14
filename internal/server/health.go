package server

import (
	"context"
	"sync"
	"time"

	"github.com/gofiber/fiber/v3"
)

// ready: Postgres answers and there is a snapshot to serve. HN being down
// does not make us unready; we keep serving the last snapshot.
func (s *Server) ready(c fiber.Ctx) bool {
	if s.shuttingDown.Load() || s.d.Cache.Get() == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(c.Context(), 2*time.Second)
	defer cancel()
	return s.d.Store.Ping(ctx) == nil
}

type check struct {
	Status    string `json:"status"`
	LatencyMS *int64 `json:"latency_ms,omitempty"`
	Error     string `json:"error,omitempty"`
}

type refresherCheck struct {
	Status             string     `json:"status"`
	Enabled            bool       `json:"enabled"`
	Running            bool       `json:"running"`
	LastStatus         string     `json:"last_status,omitempty"`
	LastStartedAt      *time.Time `json:"last_started_at,omitempty"`
	LastFinishedAt     *time.Time `json:"last_finished_at,omitempty"`
	LastError          string     `json:"last_error,omitempty"`
	ConsecutiveFailure int        `json:"consecutive_failures"`
	LatestSnapshotID   *int64     `json:"latest_snapshot_id,omitempty"`
	LatestSnapshotAgeS *int64     `json:"latest_snapshot_age_s,omitempty"`
}

type healthResponse struct {
	Status  string `json:"status"` // ok | degraded | down
	Version string `json:"version"`
	UptimeS int64  `json:"uptime_s"`
	Checks  struct {
		Postgres  check          `json:"postgres"`
		HNAPI     check          `json:"hn_api"`
		Refresher refresherCheck `json:"refresher"`
	} `json:"checks"`
}

// healthz reports every dependency. Status code is 200 for ok/degraded and
// 503 for down, so it can be used as a probe too.
func (s *Server) healthz(c fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 3*time.Second)
	defer cancel()

	var h healthResponse
	h.Version = s.d.Version
	h.UptimeS = int64(time.Since(s.started).Seconds())

	var wg sync.WaitGroup
	wg.Go(func() { h.Checks.Postgres = timed(func() error { return s.d.Store.Ping(ctx) }) })
	wg.Go(func() { h.Checks.HNAPI = s.hn.check(ctx) })
	wg.Wait()

	rc := &h.Checks.Refresher
	rc.Status = "disabled"
	if s.d.Refresher != nil {
		st := s.d.Refresher.State()
		rc.Enabled, rc.Running, rc.LastStatus = st.Enabled, st.Running, st.LastStatus
		rc.LastStartedAt, rc.LastFinishedAt, rc.LastError = st.LastStartedAt, st.LastFinishedAt, st.LastError
		rc.ConsecutiveFailure = st.ConsecutiveFailures
		switch {
		case !st.Enabled:
			rc.Status = "disabled"
		case st.ConsecutiveFailures > 0:
			rc.Status = "failing"
		default:
			rc.Status = "ok"
		}
	}
	stale := false
	latest := s.d.Cache.Get()
	if latest != nil {
		id := latest.Snapshot.ID
		rc.LatestSnapshotID = &id
		if fin := latest.Snapshot.FinishedAt; fin != nil {
			age := int64(time.Since(*fin).Seconds())
			rc.LatestSnapshotAgeS = &age
			stale = s.d.RefreshInterval > 0 && time.Since(*fin) > 3*s.d.RefreshInterval
		}
	}

	switch {
	case h.Checks.Postgres.Status != "up" || latest == nil:
		h.Status = "down"
	case h.Checks.HNAPI.Status != "up" || rc.Status == "failing" || stale:
		h.Status = "degraded"
	default:
		h.Status = "ok"
	}
	code := fiber.StatusOK
	if h.Status == "down" {
		code = fiber.StatusServiceUnavailable
	}
	return c.Status(code).JSON(h)
}

func timed(fn func() error) check {
	start := time.Now()
	err := fn()
	ms := time.Since(start).Milliseconds()
	if err != nil {
		return check{Status: "down", LatencyMS: &ms, Error: err.Error()}
	}
	return check{Status: "up", LatencyMS: &ms}
}

// hnChecker probes GET /maxitem.json at most once per ttl, so frequent
// health probes don't turn into HN traffic.
type hnChecker struct {
	probe HNProbe
	ttl   time.Duration

	mu     sync.Mutex
	last   check
	lastAt time.Time
}

func newHNChecker(p HNProbe, ttl time.Duration) *hnChecker { return &hnChecker{probe: p, ttl: ttl} }

func (h *hnChecker) check(ctx context.Context) check {
	if h.probe == nil {
		return check{Status: "unknown"}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.lastAt.IsZero() && time.Since(h.lastAt) < h.ttl {
		return h.last
	}
	h.last = timed(func() error { _, err := h.probe.MaxItem(ctx); return err })
	h.lastAt = time.Now()
	return h.last
}
