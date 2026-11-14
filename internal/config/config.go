// Package config loads service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds every tunable of the service. See README for descriptions.
type Config struct {
	Port int

	DatabaseURL   string
	DBMaxConns    int32
	DBAutoMigrate bool
	DBAutoSeed    bool

	HNBaseURL        string
	HNMaxConcurrency int
	HNRequestTimeout time.Duration
	HNMaxRetries     int

	TopStories   int
	IncludeJobs  bool
	CrawlTimeout time.Duration
	MaxDepth     int

	RefreshInterval time.Duration
	RefreshEnabled  bool
	RetentionCount  int
	RetentionAge    time.Duration

	LogLevel slog.Level
}

// Load reads configuration from the environment, applying defaults.
func Load() (Config, error) {
	l := loader{}
	c := Config{
		Port:             l.int("PORT", 8080),
		DatabaseURL:      l.str("DATABASE_URL", ""),
		DBMaxConns:       int32(l.int("DB_MAX_CONNS", 10)),
		DBAutoMigrate:    l.bool("DB_AUTO_MIGRATE", true),
		DBAutoSeed:       l.bool("DB_AUTO_SEED", false),
		HNBaseURL:        strings.TrimRight(l.str("HN_BASE_URL", "https://hacker-news.firebaseio.com/v0"), "/"),
		HNMaxConcurrency: l.int("HN_MAX_CONCURRENCY", 32),
		HNRequestTimeout: l.dur("HN_REQUEST_TIMEOUT", 5*time.Second),
		HNMaxRetries:     l.int("HN_MAX_RETRIES", 3),
		TopStories:       l.int("TOP_STORIES", 5),
		IncludeJobs:      l.bool("TOP_STORIES_INCLUDE_JOBS", false),
		CrawlTimeout:     l.dur("CRAWL_TIMEOUT", 60*time.Second),
		MaxDepth:         l.int("MAX_DEPTH", 0),
		RefreshInterval:  l.dur("REFRESH_INTERVAL", 5*time.Minute),
		RefreshEnabled:   l.bool("REFRESH_ENABLED", true),
		RetentionCount:   l.int("SNAPSHOT_RETENTION_COUNT", 288),
		RetentionAge:     l.dur("SNAPSHOT_RETENTION_AGE", 168*time.Hour),
		LogLevel:         l.level("LOG_LEVEL", slog.LevelInfo),
	}
	if len(l.errs) > 0 {
		return Config{}, errors.Join(l.errs...)
	}
	return c, c.validate()
}

func (c Config) validate() error {
	var errs []error
	check := func(ok bool, msg string) {
		if !ok {
			errs = append(errs, errors.New(msg))
		}
	}
	check(c.Port > 0 && c.Port < 65536, "PORT must be between 1 and 65535")
	check(c.DBMaxConns > 0, "DB_MAX_CONNS must be > 0")
	check(c.HNMaxConcurrency > 0, "HN_MAX_CONCURRENCY must be > 0")
	check(c.HNRequestTimeout > 0, "HN_REQUEST_TIMEOUT must be > 0")
	check(c.HNMaxRetries >= 0, "HN_MAX_RETRIES must be >= 0")
	check(c.TopStories >= 1 && c.TopStories <= 100, "TOP_STORIES must be between 1 and 100")
	check(c.CrawlTimeout > 0, "CRAWL_TIMEOUT must be > 0")
	check(c.MaxDepth >= 0, "MAX_DEPTH must be >= 0")
	check(c.RefreshInterval >= time.Second, "REFRESH_INTERVAL must be >= 1s")
	check(c.RetentionCount >= 0, "SNAPSHOT_RETENTION_COUNT must be >= 0")
	check(c.RetentionAge >= 0, "SNAPSHOT_RETENTION_AGE must be >= 0")
	return errors.Join(errs...)
}

// RequireDB returns an error when no DATABASE_URL is configured.
func (c Config) RequireDB() error {
	if c.DatabaseURL == "" {
		return errors.New("DATABASE_URL is required for this command")
	}
	return nil
}

type loader struct{ errs []error }

func (l *loader) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func (l *loader) int(key string, def int) int {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: invalid integer %q", key, v))
		return def
	}
	return n
}

func (l *loader) bool(key string, def bool) bool {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: invalid boolean %q", key, v))
		return def
	}
	return b
}

func (l *loader) dur(key string, def time.Duration) time.Duration {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: invalid duration %q", key, v))
		return def
	}
	return d
}

func (l *loader) level(key string, def slog.Level) slog.Level {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(v)); err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: invalid log level %q", key, v))
		return def
	}
	return lvl
}
