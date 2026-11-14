package store

import (
	"context"
	"fmt"
	"time"
)

// crawlLockID is the pg advisory lock key that serializes crawls across
// every process sharing the database ("HNFE").
const crawlLockID int64 = 0x484e4645

// TryCrawlLock takes the session-level crawl lock on a dedicated connection.
// ok is false when another process holds it. The returned unlock releases
// the lock and the connection; it is safe to call once.
func (s *Store) TryCrawlLock(ctx context.Context) (unlock func(), ok bool, err error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("crawl lock: %w", err)
	}
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, crawlLockID).Scan(&ok); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("crawl lock: %w", err)
	}
	if !ok {
		conn.Release()
		return nil, false, nil
	}
	return func() {
		// Unlock even if the caller's ctx is gone; if this fails, destroying
		// the connection ends the session, which releases the lock anyway.
		uctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(uctx, `SELECT pg_advisory_unlock($1)`, crawlLockID); err != nil {
			_ = conn.Conn().Close(uctx)
		}
		conn.Release()
	}, true, nil
}
