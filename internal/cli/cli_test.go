package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/hntest"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
)

func runCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	srv := hntest.New(t)
	t.Setenv("HN_BASE_URL", srv.URL)
	t.Setenv("HN_MAX_RETRIES", "0")
	t.Setenv("DATABASE_URL", "")
	var out, errOut bytes.Buffer
	err := Run(context.Background(), args, &out, &errOut)
	return out.String(), err
}

func TestSnapshotJSONWithoutDB(t *testing.T) {
	out, err := runCLI(t, "snapshot", "--limit", "2")
	if err != nil {
		t.Fatal(err)
	}
	var snap model.Snapshot
	if err := json.Unmarshal([]byte(out), &snap); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(snap.Stories) != 2 || len(snap.Authors) == 0 || snap.Status == "" {
		t.Fatalf("stories=%d authors=%d status=%q", len(snap.Stories), len(snap.Authors), snap.Status)
	}
}

func TestSnapshotTextToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.txt")
	out, err := runCLI(t, "snapshot", "--format", "text", "--out", path, "--limit", "2")
	if err != nil {
		t.Fatal(err)
	}
	if out != "" {
		t.Fatalf("stdout should be empty with --out, got %q", out)
	}
	b, _ := os.ReadFile(path)
	text := string(b)
	for _, want := range []string{"Snapshot", "[1]", "[2]", "├─"} {
		if !strings.Contains(text, want) {
			t.Errorf("text output lacks %q:\n%s", want, text)
		}
	}
}

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{"bogus"},
		{"snapshot", "--limit", "0"},
		{"snapshot", "--format", "xml"},
		{"snapshot", "extra"},
		{"crawl", "--limit", "500"},
		{"serve", "--nope"},
	} {
		if _, err := runCLI(t, args...); !errors.Is(err, ErrUsage) {
			t.Errorf("%v: err = %v, want ErrUsage", args, err)
		}
	}
}

func TestDBCommandsNeedDatabaseURL(t *testing.T) {
	for _, args := range [][]string{{"crawl"}, {"migrate", "status"}, {"seed", "up"}, {"snapshot", "--from-db"}, {"serve"}} {
		_, err := runCLI(t, args...)
		if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
			t.Errorf("%v: err = %v, want DATABASE_URL error", args, err)
		}
	}
}

func TestVersion(t *testing.T) {
	out, err := runCLI(t, "version")
	if err != nil || strings.TrimSpace(out) != Version {
		t.Fatalf("%q %v", out, err)
	}
}
