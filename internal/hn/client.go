// Package hn is a small, concurrency-bounded client for the HackerNews API.
package hn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Options configures a Client.
type Options struct {
	BaseURL        string
	MaxConcurrency int
	RequestTimeout time.Duration
	MaxRetries     int
	// BackoffBase is the first retry delay; it doubles each attempt (with jitter).
	BackoffBase time.Duration
	HTTPClient  *http.Client
}

// Client fetches HN resources. All requests share one semaphore, so
// MaxConcurrency is a hard cap on in-flight requests regardless of how many
// goroutines call the client.
type Client struct {
	base        string
	http        *http.Client
	sem         chan struct{}
	timeout     time.Duration
	maxRetries  int
	backoffBase time.Duration
}

// FetchError is returned when a resource could not be fetched after retries.
type FetchError struct {
	Path     string
	Attempts int
	Err      error
}

func (e *FetchError) Error() string {
	return fmt.Sprintf("GET %s failed after %d attempt(s): %v", e.Path, e.Attempts, e.Err)
}

func (e *FetchError) Unwrap() error { return e.Err }

type statusError struct{ code int }

func (e statusError) Error() string { return "http " + strconv.Itoa(e.code) }

// New builds a Client with sane defaults for zero-valued options.
func New(o Options) *Client {
	if o.MaxConcurrency <= 0 {
		o.MaxConcurrency = 32
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 5 * time.Second
	}
	if o.BackoffBase <= 0 {
		o.BackoffBase = 200 * time.Millisecond
	}
	if o.HTTPClient == nil {
		o.HTTPClient = &http.Client{Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:        o.MaxConcurrency * 2,
			MaxIdleConnsPerHost: o.MaxConcurrency,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 5 * time.Second,
			ForceAttemptHTTP2:   true,
		}}
	}
	return &Client{
		base:        o.BaseURL,
		http:        o.HTTPClient,
		sem:         make(chan struct{}, o.MaxConcurrency),
		timeout:     o.RequestTimeout,
		maxRetries:  o.MaxRetries,
		backoffBase: o.BackoffBase,
	}
}

// TopStories returns the ranked ids from /topstories (stories and jobs).
func (c *Client) TopStories(ctx context.Context) ([]int64, error) {
	var ids []int64
	if _, err := c.getJSON(ctx, "/topstories.json", &ids); err != nil {
		return nil, err
	}
	return ids, nil
}

// Item returns the item, or (nil, nil) when HN returns null for it.
func (c *Client) Item(ctx context.Context, id int64) (*Item, error) {
	var it Item
	found, err := c.getJSON(ctx, "/item/"+strconv.FormatInt(id, 10)+".json", &it)
	if err != nil || !found {
		return nil, err
	}
	return &it, nil
}

// User returns the profile, or (nil, nil) when HN returns null for it.
// Usernames are case-sensitive and passed through untouched.
func (c *Client) User(ctx context.Context, username string) (*User, error) {
	var u User
	found, err := c.getJSON(ctx, "/user/"+url.PathEscape(username)+".json", &u)
	if err != nil || !found {
		return nil, err
	}
	return &u, nil
}

// MaxItem returns the current largest item id. Used as a cheap reachability probe.
func (c *Client) MaxItem(ctx context.Context) (int64, error) {
	var id int64
	if _, err := c.getJSON(ctx, "/maxitem.json", &id); err != nil {
		return 0, err
	}
	return id, nil
}

// getJSON performs a GET with retries. It reports found=false for a JSON
// `null` body (HN's way of saying "no such resource") or a 404.
func (c *Client) getJSON(ctx context.Context, path string, dst any) (found bool, err error) {
	var lastErr error
	attempts := 0
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			if err := sleep(ctx, c.backoff(attempt)); err != nil {
				return false, &FetchError{Path: path, Attempts: attempts, Err: err}
			}
		}
		attempts++
		body, status, err := c.do(ctx, path)
		switch {
		case err != nil:
			lastErr = err
			if ctx.Err() != nil { // caller gave up; retrying is pointless
				return false, &FetchError{Path: path, Attempts: attempts, Err: ctx.Err()}
			}
			continue
		case status == http.StatusNotFound:
			return false, nil
		case status == http.StatusTooManyRequests || status >= 500:
			lastErr = statusError{status}
			continue
		case status != http.StatusOK:
			return false, &FetchError{Path: path, Attempts: attempts, Err: statusError{status}}
		}
		if bytes.Equal(bytes.TrimSpace(body), []byte("null")) {
			return false, nil
		}
		if err := json.Unmarshal(body, dst); err != nil {
			return false, &FetchError{Path: path, Attempts: attempts, Err: fmt.Errorf("decode: %w", err)}
		}
		return true, nil
	}
	return false, &FetchError{Path: path, Attempts: attempts, Err: lastErr}
}

func (c *Client) do(ctx context.Context, path string) ([]byte, int, error) {
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
	defer func() { <-c.sem }()

	rctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "hnfetcher/1.0")
	resp, err := c.http.Do(req)
	if err != nil {
		if errors.Is(rctx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			return nil, 0, fmt.Errorf("request timeout after %s", c.timeout)
		}
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, 0, err
	}
	return body, resp.StatusCode, nil
}

func (c *Client) backoff(attempt int) time.Duration {
	d := c.backoffBase << (attempt - 1)
	return d/2 + rand.N(d/2+1) // jitter in [d/2, d]
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
