// Package server exposes snapshots over HTTP with Fiber v3.
package server

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/compress"
	"github.com/gofiber/fiber/v3/middleware/healthcheck"
	"github.com/gofiber/fiber/v3/middleware/limiter"
	"github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/gofiber/fiber/v3/middleware/requestid"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/refresher"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/store"
)

// Store is the read side of the database the API needs.
type Store interface {
	Ping(ctx context.Context) error
	Snapshot(ctx context.Context, id int64) (*model.Snapshot, error)
	ListSnapshots(ctx context.Context, o store.ListOptions) ([]model.SnapshotSummary, error)
}

// Refresher is the background crawler as seen by the API.
type Refresher interface {
	Trigger() error
	State() refresher.State
}

// HNProbe checks whether the HackerNews API answers.
type HNProbe interface {
	MaxItem(ctx context.Context) (int64, error)
}

// Deps wires the server to the rest of the service.
type Deps struct {
	Store           Store
	Cache           *refresher.Cache
	Refresher       Refresher
	HN              HNProbe
	Version         string
	RefreshInterval time.Duration
	RefreshLimit    int // POST /v1/snapshots/refresh per client per minute (default 5)
	Log             *slog.Logger
}

// Server is the HTTP API.
type Server struct {
	App *fiber.App

	d            Deps
	log          *slog.Logger
	started      time.Time
	shuttingDown atomic.Bool
	hn           *hnChecker
}

// New builds the Fiber app with all routes and middleware.
func New(d Deps) *Server {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Cache == nil {
		d.Cache = &refresher.Cache{}
	}
	if d.RefreshLimit <= 0 {
		d.RefreshLimit = 5
	}
	s := &Server{d: d, log: d.Log.With("component", "http"), started: time.Now(), hn: newHNChecker(d.HN, 10*time.Second)}

	app := fiber.New(fiber.Config{
		AppName:      "hnfetcher " + d.Version,
		ErrorHandler: s.errorHandler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	})
	app.Use(recover.New())
	app.Use(requestid.New())
	app.Use(s.accessLog)

	// Health endpoints are registered before compress: tiny bodies, hit often.
	app.Get(healthcheck.LivenessEndpoint, healthcheck.New(healthcheck.Config{
		Probe: func(fiber.Ctx) bool { return !s.shuttingDown.Load() },
	}))
	app.Get(healthcheck.ReadinessEndpoint, healthcheck.New(healthcheck.Config{Probe: s.ready}))
	app.Get("/healthz", s.healthz)

	app.Use(compress.New())
	v1 := app.Group("/v1")
	v1.Get("/top-stories", s.topStories)
	v1.Get("/snapshots", s.listSnapshots)
	v1.Post("/snapshots/refresh", limiter.New(limiter.Config{
		Max:        d.RefreshLimit,
		Expiration: time.Minute,
		LimitReached: func(c fiber.Ctx) error {
			return apiError(fiber.StatusTooManyRequests, "rate_limited", "too many refresh requests; try again in a minute")
		},
	}), s.refresh)
	v1.Get("/snapshots/:id", s.snapshot)
	v1.Get("/snapshots/:id/stories/:story_id", s.story)
	v1.Get("/authors/:username", s.author)

	app.Use(func(c fiber.Ctx) error {
		return apiError(fiber.StatusNotFound, "not_found", "no route for "+c.Method()+" "+c.Path())
	})
	s.App = app
	return s
}

// Listen serves on addr until Shutdown is called.
func (s *Server) Listen(addr string) error {
	return s.App.Listen(addr, fiber.ListenConfig{DisableStartupMessage: true})
}

// Shutdown makes the health probes fail, then drains connections.
func (s *Server) Shutdown(ctx context.Context) error {
	s.shuttingDown.Store(true)
	return s.App.ShutdownWithContext(ctx)
}

// MarkShuttingDown flips /livez and /readyz to 503 without stopping.
func (s *Server) MarkShuttingDown() { s.shuttingDown.Store(true) }

func (s *Server) accessLog(c fiber.Ctx) error {
	start := time.Now()
	err := c.Next()
	status := c.Response().StatusCode()
	if err != nil {
		var fe *fiber.Error
		if errors.As(err, &fe) {
			status = fe.Code
		} else {
			status = fiber.StatusInternalServerError
		}
	}
	level := slog.LevelInfo
	switch {
	case status >= 500:
		level = slog.LevelError
	case c.Path() == healthcheck.LivenessEndpoint || c.Path() == healthcheck.ReadinessEndpoint:
		level = slog.LevelDebug // probes every few seconds would drown the log
	}
	s.log.Log(c.Context(), level, "request",
		"method", c.Method(), "path", c.Path(), "status", status,
		"duration_ms", time.Since(start).Milliseconds(), "request_id", requestid.FromContext(c))
	return err
}
