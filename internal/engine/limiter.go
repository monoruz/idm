package engine

import (
	"context"
	"sync"
	"time"
)

// limiter is a token bucket shared by every connection of every download in a
// queue. Tokens may go negative (debt), which keeps the long-run rate exact
// even when reads are larger than the remaining budget.
type limiter struct {
	mu     sync.Mutex
	rate   float64 // bytes per second, 0 = unlimited
	tokens float64
	last   time.Time
}

func newLimiter(rate int64) *limiter {
	l := &limiter{last: time.Now()}
	l.setRate(rate)
	return l
}

func (l *limiter) setRate(rate int64) {
	l.mu.Lock()
	l.rate = float64(rate)
	l.tokens = 0
	l.last = time.Now()
	l.mu.Unlock()
}

func (l *limiter) burst() float64 { return max(l.rate/4, 16<<10) }

// wait accounts for n bytes and blocks until the bucket is out of debt.
func (l *limiter) wait(ctx context.Context, n int) error {
	l.mu.Lock()
	if l.rate <= 0 {
		l.mu.Unlock()
		return nil
	}
	now := time.Now()
	l.tokens = min(l.burst(), l.tokens+now.Sub(l.last).Seconds()*l.rate)
	l.last = now
	l.tokens -= float64(n)
	var d time.Duration
	if l.tokens < 0 {
		d = time.Duration(-l.tokens / l.rate * float64(time.Second))
	}
	l.mu.Unlock()
	if d == 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
