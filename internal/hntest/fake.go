// Package hntest provides a fake HackerNews API server for tests.
package hntest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Fixture is the on-disk fake dataset.
type Fixture struct {
	TopStories []int64                    `json:"topstories"`
	Items      map[string]json.RawMessage `json:"items"`
	Users      map[string]json.RawMessage `json:"users"`
}

// Server is a fake HN API. Unknown items/users are served as `null`.
type Server struct {
	*httptest.Server
	fx Fixture

	mu        sync.Mutex
	hits      map[string]int
	fail      map[string]bool          // path -> always HTTP 500
	failTimes map[string]int           // path -> fail N times, then succeed
	delay     map[string]time.Duration // path -> artificial latency
	Latency   time.Duration            // applied to every request

	inFlight    atomic.Int64
	MaxInFlight atomic.Int64
}

// LoadFixture reads testdata/hn_fixture.json from the repo root.
func LoadFixture(t testing.TB) Fixture {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(file), "..", "..", "testdata", "hn_fixture.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fx Fixture
	if err := json.Unmarshal(b, &fx); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	return fx
}

// New starts a fake server with the standard fixture, where item 104 always
// fails with HTTP 500 (to produce a status=error node).
func New(t testing.TB) *Server {
	t.Helper()
	s := NewWithFixture(t, LoadFixture(t))
	s.FailAlways("/item/104.json")
	return s
}

// NewWithFixture starts a fake server for an arbitrary fixture.
func NewWithFixture(t testing.TB, fx Fixture) *Server {
	t.Helper()
	s := &Server{
		fx: fx, hits: map[string]int{}, fail: map[string]bool{},
		failTimes: map[string]int{}, delay: map[string]time.Duration{},
	}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(s.Close)
	return s
}

// FailAlways makes path respond with HTTP 500 forever.
func (s *Server) FailAlways(path string) { s.mu.Lock(); s.fail[path] = true; s.mu.Unlock() }

// FailTimes makes path respond with HTTP 500 n times, then normally.
func (s *Server) FailTimes(path string, n int) { s.mu.Lock(); s.failTimes[path] = n; s.mu.Unlock() }

// Delay adds latency to one path.
func (s *Server) Delay(path string, d time.Duration) { s.mu.Lock(); s.delay[path] = d; s.mu.Unlock() }

// Hits returns how many times path was requested.
func (s *Server) Hits(path string) int { s.mu.Lock(); defer s.mu.Unlock(); return s.hits[path] }

// HitsWithPrefix sums hits for all paths with the prefix.
func (s *Server) HitsWithPrefix(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for p, c := range s.hits {
		if strings.HasPrefix(p, prefix) {
			n += c
		}
	}
	return n
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	cur := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	for {
		m := s.MaxInFlight.Load()
		if cur <= m || s.MaxInFlight.CompareAndSwap(m, cur) {
			break
		}
	}

	path := r.URL.Path
	s.mu.Lock()
	s.hits[path]++
	failAlways := s.fail[path]
	failNow := s.failTimes[path] > 0
	if failNow {
		s.failTimes[path]--
	}
	d := s.delay[path] + s.Latency
	s.mu.Unlock()

	if d > 0 {
		select {
		case <-time.After(d):
		case <-r.Context().Done():
			return
		}
	}
	if failAlways || failNow {
		http.Error(w, "boom", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	switch {
	case path == "/topstories.json":
		_ = json.NewEncoder(w).Encode(s.fx.TopStories)
	case path == "/maxitem.json":
		_, _ = w.Write([]byte("99999"))
	case strings.HasPrefix(path, "/item/"):
		writeRaw(w, s.fx.Items[strings.TrimSuffix(strings.TrimPrefix(path, "/item/"), ".json")])
	case strings.HasPrefix(path, "/user/"):
		writeRaw(w, s.fx.Users[strings.TrimSuffix(strings.TrimPrefix(path, "/user/"), ".json")])
	default:
		http.NotFound(w, r)
	}
}

func writeRaw(w http.ResponseWriter, raw json.RawMessage) {
	if raw == nil {
		_, _ = w.Write([]byte("null"))
		return
	}
	_, _ = w.Write(raw)
}
