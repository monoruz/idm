package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"idm/internal/store"
)

const (
	bufSize     = 32 << 10
	minSplit    = 512 << 10 // never create a segment smaller than this
	idleTimeout = 30 * time.Second
	maxRetries  = 6
	partSuffix  = ".idmpart"
)

var bufPool = sync.Pool{New: func() any { b := make([]byte, bufSize); return &b }}

var errBadSource = errors.New("source does not serve the expected byte range")

type seg struct {
	start, end, pos int64 // end == -1: unknown size
	active          bool
	finished        bool // reached EOF on an unknown-size stream
}

func (s *seg) done() bool {
	if s.end < 0 {
		return s.finished
	}
	return s.pos >= s.end
}

// task is one running download. Its fields are owned by the task goroutines;
// the manager only reads them through snapshot() and the atomics.
type task struct {
	id      uint64
	m       *Manager
	ctx     context.Context
	cancel  context.CancelFunc
	lim     atomic.Pointer[limiter]
	stopAs  atomic.Value // store.Status to apply when cancelled
	restart bool         // resumed while stopping; guarded by Manager.mu
	doneCh  chan struct{}
	started time.Time

	downloaded atomic.Int64
	speed      atomic.Int64 // bytes/s, maintained by the manager ticker
	lastSample int64

	mu        sync.Mutex
	segs      []*seg
	size      int64
	resumable bool
	file      *os.File
	sources   []string
	srcFails  map[string]int
	lastErr   error
}

func (t *task) stop(as store.Status) {
	t.stopAs.Store(as)
	t.cancel()
}

// result is reported back to the manager when the task ends.
type result struct {
	completed bool
	finalName string
	err       error
}

func (t *task) run(d store.Download, conns int) {
	defer close(t.doneCh)
	res := t.download(d, conns)
	if t.file != nil {
		t.file.Close()
	}
	t.m.finish(t, res)
}

func (t *task) download(d store.Download, conns int) result {
	ref, sources, err := t.probeSources(d)
	if err != nil {
		return result{err: err}
	}
	t.sources = sources
	t.srcFails = map[string]int{}
	t.size, t.resumable = ref.size, ref.resumable

	// The manager settles the target folder/name on first start.
	d, err = t.m.prepare(t.id, ref)
	if err != nil {
		return result{err: err}
	}
	if err := os.MkdirAll(d.Dir, 0o755); err != nil {
		return result{err: err}
	}
	partPath := joinPath(d.Dir, d.FileName) + partSuffix

	reuse := len(d.Segments) > 0 && d.Resumable && ref.resumable && d.Size == ref.size &&
		(d.Validator == "" || ref.validator == "" || d.Validator == ref.validator)
	if reuse {
		if _, err := os.Stat(partPath); err != nil {
			reuse = false
		}
	}
	flags := os.O_RDWR | os.O_CREATE
	if !reuse {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(partPath, flags, 0o644)
	if err != nil {
		return result{err: err}
	}
	t.file = f

	if reuse {
		// Only unfinished segments are persisted, so derive progress from
		// what is still missing.
		remaining := int64(0)
		for _, s := range d.Segments {
			t.segs = append(t.segs, &seg{start: s.Start, end: s.End, pos: s.Pos})
			remaining += s.End - s.Pos
		}
		t.downloaded.Store(t.size - remaining)
	} else {
		if t.size > 0 {
			f.Truncate(t.size) // sparse preallocation
		}
		t.segs = initialSegments(t.size, t.resumable, conns)
		t.downloaded.Store(0)
	}
	t.lastSample = t.downloaded.Load()
	segs, _ := t.state()
	t.m.updateMeta(t.id, ref, segs)

	workers := conns
	if !t.resumable {
		workers = 1
	}
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.worker(i)
		}()
	}
	wg.Wait()

	if t.ctx.Err() != nil {
		return result{}
	}
	t.mu.Lock()
	complete := true
	for _, s := range t.segs {
		if !s.done() {
			complete = false
		}
	}
	lastErr := t.lastErr
	t.mu.Unlock()
	if !complete {
		if lastErr == nil {
			lastErr = errors.New("download incomplete")
		}
		return result{err: lastErr}
	}
	if err := f.Sync(); err != nil {
		return result{err: err}
	}
	f.Close()
	t.file = nil
	final := uniqueName(d.FileName, func(c string) bool { return exists(joinPath(d.Dir, c)) })
	if err := os.Rename(partPath, joinPath(d.Dir, final)); err != nil {
		return result{err: err}
	}
	return result{completed: true, finalName: final}
}

// probeSources checks the primary URL and all mirrors concurrently. The first
// healthy URL in user order is the reference; mirrors are only used in
// parallel when they serve the same file size with range support.
func (t *task) probeSources(d store.Download) (*probeResult, []string, error) {
	urls := append([]string{d.URL}, d.Mirrors...)
	results := make([]*probeResult, len(urls))
	errs := make([]error, len(urls))
	var wg sync.WaitGroup
	for i, u := range urls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = probe(t.ctx, t.m.client, u)
		}()
	}
	wg.Wait()
	if t.ctx.Err() != nil {
		return nil, nil, t.ctx.Err()
	}
	var ref *probeResult
	for _, r := range results {
		if r != nil {
			ref = r
			break
		}
	}
	if ref == nil {
		return nil, nil, fmt.Errorf("no source reachable: %w", errs[0])
	}
	sources := []string{ref.url}
	if ref.resumable && ref.size > 0 {
		for _, r := range results {
			if r != nil && r != ref && r.resumable && r.size == ref.size {
				sources = append(sources, r.url)
			}
		}
	}
	return ref, sources, nil
}

func initialSegments(size int64, resumable bool, conns int) []*seg {
	if !resumable || size <= 0 {
		return []*seg{{start: 0, end: size}}
	}
	n := int64(conns)
	if maxN := size / minSplit; maxN < n {
		n = max(maxN, 1)
	}
	segs := make([]*seg, 0, n)
	chunk := size / n
	for i := range n {
		start := i * chunk
		end := start + chunk
		if i == n-1 {
			end = size
		}
		segs = append(segs, &seg{start: start, end: end, pos: start})
	}
	return segs
}

// claim hands out an idle unfinished segment, or splits the largest active one
// in half so fast connections keep working until the very end.
func (t *task) claim() *seg {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, s := range t.segs {
		if !s.active && !s.done() {
			s.active = true
			return s
		}
	}
	if !t.resumable {
		return nil
	}
	var big *seg
	for _, s := range t.segs {
		if s.active && s.end-s.pos > 2*minSplit && (big == nil || s.end-s.pos > big.end-big.pos) {
			big = s
		}
	}
	if big == nil {
		return nil
	}
	mid := big.pos + (big.end-big.pos)/2
	ns := &seg{start: mid, end: big.end, pos: mid, active: true}
	big.end = mid
	t.segs = append(t.segs, ns)
	return ns
}

func (t *task) aliveSources() []string {
	var out []string
	for _, s := range t.sources {
		if t.srcFails[s] >= 0 {
			out = append(out, s)
		}
	}
	return out
}

func (t *task) worker(i int) {
	srcIdx := i
	failures := 0
	for t.ctx.Err() == nil {
		t.mu.Lock()
		alive := t.aliveSources()
		t.mu.Unlock()
		if len(alive) == 0 {
			return
		}
		src := alive[srcIdx%len(alive)]

		s := t.claim()
		if s == nil {
			return
		}
		err := t.fetch(s, src)
		t.mu.Lock()
		s.active = false
		if err == nil || t.ctx.Err() != nil {
			t.mu.Unlock()
			failures = 0
			continue
		}
		t.lastErr = err
		if errors.Is(err, errBadSource) && len(t.sources) > 1 {
			t.srcFails[src] = -1 // drop this mirror for the rest of the run
		}
		t.mu.Unlock()

		failures++
		if failures > maxRetries {
			return
		}
		srcIdx++ // fail over to the next source
		select {
		case <-time.After(time.Duration(failures) * time.Second):
		case <-t.ctx.Done():
			return
		}
	}
}

func (t *task) fetch(s *seg, src string) error {
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	idle := time.AfterFunc(idleTimeout, cancel)
	defer idle.Stop()

	t.mu.Lock()
	if !t.resumable && s.pos > 0 {
		// Server cannot resume: start over.
		t.downloaded.Add(-s.pos)
		s.pos = 0
	}
	from, to := s.pos, s.end
	t.mu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
	if err != nil {
		return err
	}
	if t.resumable {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(from, 10)+"-"+strconv.FormatInt(to-1, 10))
	}
	resp, err := t.m.client.Do(req)
	if err != nil {
		return t.idleErr(err)
	}
	defer resp.Body.Close()
	if t.resumable {
		if resp.StatusCode != http.StatusPartialContent ||
			!strings.HasPrefix(resp.Header.Get("Content-Range"), "bytes "+strconv.FormatInt(from, 10)+"-") {
			return fmt.Errorf("%w (%s)", errBadSource, resp.Status)
		}
	} else if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned %s", resp.Status)
	}

	bp := bufPool.Get().(*[]byte)
	defer bufPool.Put(bp)
	buf := *bp
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			idle.Reset(idleTimeout)
			t.mu.Lock()
			pos, k := s.pos, int64(n)
			if s.end >= 0 {
				k = min(k, s.end-pos)
			}
			t.mu.Unlock()
			if k > 0 {
				if _, err := t.file.WriteAt(buf[:k], pos); err != nil {
					return err
				}
				t.mu.Lock()
				s.pos += k
				t.mu.Unlock()
				t.downloaded.Add(k)
			}
			if err := t.lim.Load().wait(t.ctx, int(k)); err != nil {
				return err
			}
			t.mu.Lock()
			reached := s.end >= 0 && s.pos >= s.end
			t.mu.Unlock()
			if reached {
				return nil // also covers a segment shortened by a split
			}
		}
		if rerr == io.EOF {
			t.mu.Lock()
			defer t.mu.Unlock()
			if s.end < 0 {
				s.finished = true
				return nil
			}
			if s.pos >= s.end {
				return nil
			}
			return io.ErrUnexpectedEOF
		}
		if rerr != nil {
			return t.idleErr(rerr)
		}
	}
}

func (t *task) idleErr(err error) error {
	if t.ctx.Err() == nil && errors.Is(err, context.Canceled) {
		return errors.New("connection stalled")
	}
	return err
}

// state returns the unfinished byte ranges for checkpointing.
func (t *task) state() ([]store.Segment, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.resumable {
		return nil, false
	}
	out := make([]store.Segment, 0, len(t.segs))
	for _, s := range t.segs {
		if !s.done() {
			out = append(out, store.Segment{Start: s.start, End: s.end, Pos: s.pos})
		}
	}
	return out, true
}
