package hn_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/hn"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/hntest"
)

func newClient(url string, retries int) *hn.Client {
	return hn.New(hn.Options{BaseURL: url, MaxConcurrency: 4, RequestTimeout: time.Second, MaxRetries: retries, BackoffBase: time.Millisecond})
}

func TestItemDecodesAndIgnoresUnknownFields(t *testing.T) {
	srv := hntest.New(t)
	it, err := newClient(srv.URL, 0).Item(context.Background(), 107)
	if err != nil || it == nil {
		t.Fatalf("Item(107) = %v, %v", it, err)
	}
	if it.By != "alice" || it.Parent != 105 || it.Time != 1762956600 {
		t.Fatalf("unexpected item: %+v", it)
	}
}

func TestNullIsNotFoundAndNotRetried(t *testing.T) {
	srv := hntest.New(t)
	c := newClient(srv.URL, 3)
	it, err := c.Item(context.Background(), 103)
	if err != nil || it != nil {
		t.Fatalf("Item(103) = %v, %v; want nil, nil", it, err)
	}
	if h := srv.Hits("/item/103.json"); h != 1 {
		t.Fatalf("null item fetched %d times, want 1", h)
	}
	u, err := c.User(context.Background(), "ghost")
	if err != nil || u != nil {
		t.Fatalf("User(ghost) = %v, %v; want nil, nil", u, err)
	}
}

func TestRetriesTransientErrors(t *testing.T) {
	srv := hntest.New(t)
	srv.FailTimes("/item/101.json", 2)
	it, err := newClient(srv.URL, 3).Item(context.Background(), 101)
	if err != nil || it == nil {
		t.Fatalf("Item(101) = %v, %v", it, err)
	}
	if h := srv.Hits("/item/101.json"); h != 3 {
		t.Fatalf("hits = %d, want 3", h)
	}
}

func TestGivesUpAfterMaxRetries(t *testing.T) {
	srv := hntest.New(t) // 104 always fails
	_, err := newClient(srv.URL, 2).Item(context.Background(), 104)
	var fe *hn.FetchError
	if !errors.As(err, &fe) || fe.Attempts != 3 {
		t.Fatalf("err = %v; want FetchError with 3 attempts", err)
	}
	if h := srv.Hits("/item/104.json"); h != 3 {
		t.Fatalf("hits = %d, want 3", h)
	}
}

func TestRequestTimeout(t *testing.T) {
	srv := hntest.New(t)
	srv.Delay("/item/100.json", 500*time.Millisecond)
	c := hn.New(hn.Options{BaseURL: srv.URL, RequestTimeout: 50 * time.Millisecond, BackoffBase: time.Millisecond})
	if _, err := c.Item(context.Background(), 100); err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestUsernamesAreCaseSensitive(t *testing.T) {
	srv := hntest.New(t)
	c := newClient(srv.URL, 0)
	if u, _ := c.User(context.Background(), "Alice"); u != nil {
		t.Fatal("Alice should not resolve to alice")
	}
	if u, _ := c.User(context.Background(), "alice"); u == nil || u.Karma != 4200 {
		t.Fatalf("alice = %+v", u)
	}
}
