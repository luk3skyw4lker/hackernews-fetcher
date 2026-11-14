//go:build integration

package crawler_test

import (
	"context"
	"testing"
	"time"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/crawler"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/hn"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
)

// TestRealHackerNews crawls the live API (top 1 story). Run with
// `go test -tags integration ./internal/crawler/`.
func TestRealHackerNews(t *testing.T) {
	client := hn.New(hn.Options{BaseURL: "https://hacker-news.firebaseio.com/v0", MaxConcurrency: 16, RequestTimeout: 10 * time.Second, MaxRetries: 2})
	snap, err := crawler.New(client, crawler.Options{TopStories: 1, Timeout: 60 * time.Second}, nil).Crawl(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Stories) != 1 || snap.Stories[0].Tree.Status != model.StatusOK {
		t.Fatalf("unexpected snapshot: %+v", snap.Stats)
	}
	if by := snap.Stories[0].Tree.By; snap.Authors[by] == nil {
		t.Fatalf("story author %q not in authors map", by)
	}
	t.Logf("status=%s items=%d authors=%d missing=%d failed=%d",
		snap.Status, snap.Stats.ItemsFetched, snap.Stats.AuthorsFetched, snap.Stats.MissingItems, snap.Stats.FailedItems)
}
