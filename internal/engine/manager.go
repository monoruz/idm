// Package engine runs downloads: queue scheduling, segmented multi-source
// transfers, shared per-queue rate limits and progress reporting.
package engine

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"idm/internal/store"
)

const (
	MaxConnections = 32
	saveEvery      = 3 // ticks between progress checkpoints
)

type Event struct {
	Kind string // "refresh" or "progress"
	Data any
}

type Progress struct {
	ID    uint64 `json:"id"`
	Done  int64  `json:"done"`
	Size  int64  `json:"size"`
	Speed int64  `json:"speed"`
	ETA   int64  `json:"eta"` // seconds, -1 unknown
}

type ProgressEvent struct {
	Items []Progress `json:"items"`
	Total int64      `json:"total"`
}

type DownloadView struct {
	store.Download
	Speed     int64
	ETA       int64
	Active    bool
	QueueName string
}

func (v DownloadView) Percent() float64 {
	if v.Size <= 0 {
		return 0
	}
	return float64(v.Downloaded) * 100 / float64(v.Size)
}

type QueueView struct {
	store.Queue
	Active, Pending, Total int
}

type AddLink struct {
	URL     string
	Mirrors []string
	Dir     string // raw user input, resolved by the manager
}

type AddRequest struct {
	Links       []AddLink
	BatchDir    string // optional folder for links without their own Dir
	QueueID     uint64
	Connections int
	Start       bool
}

type UpdateRequest struct {
	Mirrors     []string
	Connections int
	QueueID     uint64
	Dir         *string
	FileName    *string
}

type Manager struct {
	st          *store.Store
	client      *http.Client
	allowAnyDir bool

	mu        sync.Mutex
	settings  store.Settings
	downloads map[uint64]*store.Download
	queues    map[uint64]*store.Queue
	limiters  map[uint64]*limiter
	tasks     map[uint64]*task
	closed    bool

	subsMu sync.Mutex
	subs   map[chan Event]struct{}
}

func New(st *store.Store, settings *store.Settings, allowAnyDir bool) (*Manager, error) {
	m := &Manager{
		st:          st,
		allowAnyDir: allowAnyDir,
		settings:    *settings,
		downloads:   map[uint64]*store.Download{},
		queues:      map[uint64]*store.Queue{},
		limiters:    map[uint64]*limiter{},
		tasks:       map[uint64]*task{},
		subs:        map[chan Event]struct{}{},
		client:      newClient(),
	}
	queues, err := st.Queues()
	if err != nil {
		return nil, err
	}
	if len(queues) == 0 {
		queues = []*store.Queue{
			{Name: "High", MaxConcurrent: 1, Default: true, Builtin: true},
			{Name: "Low", SpeedLimit: 500 << 10, MaxConcurrent: 1, Builtin: true},
		}
		for _, q := range queues {
			if err := st.SaveQueue(q); err != nil {
				return nil, err
			}
		}
	}
	for _, q := range queues {
		m.queues[q.ID] = q
		m.limiters[q.ID] = newLimiter(q.SpeedLimit)
	}
	downloads, err := st.Downloads()
	if err != nil {
		return nil, err
	}
	for _, d := range downloads {
		if d.Status == store.StatusDownloading {
			d.Status = store.StatusQueued
		}
		if m.queues[d.QueueID] == nil {
			d.QueueID = m.defaultQueueLocked().ID
		}
		m.downloads[d.ID] = d
	}
	m.mu.Lock()
	m.scheduleLocked()
	m.mu.Unlock()
	go m.ticker()
	return m, nil
}

func newClient() *http.Client {
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 20 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		MaxIdleConnsPerHost:   MaxConnections,
		IdleConnTimeout:       60 * time.Second,
		// Byte ranges must map to raw bytes on disk.
		DisableCompression: true,
		// HTTP/2 would multiplex every "connection" onto one TCP stream,
		// defeating per-connection server throttling.
		TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	return &http.Client{Transport: &uaTransport{tr}}
}

type uaTransport struct{ rt http.RoundTripper }

func (u *uaTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Header.Get("User-Agent") == "" {
		r.Header.Set("User-Agent", "Mozilla/5.0 (compatible; idm/1.0)")
	}
	return u.rt.RoundTrip(r)
}

// Close stops all transfers, keeping them queued for the next start.
func (m *Manager) Close() {
	m.mu.Lock()
	m.closed = true
	tasks := make([]*task, 0, len(m.tasks))
	for _, t := range m.tasks {
		t.stop(store.StatusQueued)
		tasks = append(tasks, t)
	}
	m.mu.Unlock()
	for _, t := range tasks {
		<-t.doneCh
	}
}

// ---- events ----

func (m *Manager) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 16)
	m.subsMu.Lock()
	m.subs[ch] = struct{}{}
	m.subsMu.Unlock()
	return ch, func() {
		m.subsMu.Lock()
		delete(m.subs, ch)
		m.subsMu.Unlock()
	}
}

func (m *Manager) publish(e Event) {
	m.subsMu.Lock()
	defer m.subsMu.Unlock()
	for ch := range m.subs {
		select {
		case ch <- e:
		default: // slow client; it will catch up on the next event
		}
	}
}

func (m *Manager) refresh() { m.publish(Event{Kind: "refresh"}) }

// ---- settings ----

func (m *Manager) Settings() store.Settings {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.settings
	s.Categories = slices.Clone(s.Categories)
	return s
}

func (m *Manager) UpdateSettings(fn func(*store.Settings) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.settings
	if err := fn(&s); err != nil {
		return err
	}
	if err := m.st.SaveSettings(&s); err != nil {
		return err
	}
	m.settings = s
	return nil
}

// ResolveDir turns user input into an absolute folder. Relative paths live
// under the base download folder, which is also the boundary for absolute
// paths unless the server was started with -allow-any-dir.
func (m *Manager) ResolveDir(in string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resolveDirLocked(in)
}

func (m *Manager) resolveDirLocked(in string) (string, error) {
	in = strings.TrimSpace(in)
	if in == "" {
		return "", nil
	}
	base := filepath.Clean(m.settings.BaseDir)
	var p string
	if filepath.IsAbs(in) {
		p = filepath.Clean(in)
	} else {
		p = filepath.Join(base, in)
	}
	if !m.allowAnyDir && p != base && !strings.HasPrefix(p, base+string(filepath.Separator)) {
		return "", fmt.Errorf("folder %q is outside the download folder %s", in, base)
	}
	return p, nil
}

// ---- views ----

func (m *Manager) viewLocked(d *store.Download) DownloadView {
	v := DownloadView{Download: *d, ETA: -1}
	v.Segments = nil
	if q := m.queues[d.QueueID]; q != nil {
		v.QueueName = q.Name
	}
	if t := m.tasks[d.ID]; t != nil {
		v.Active = true
		v.Downloaded = t.downloaded.Load()
		v.Speed = t.speed.Load()
		v.ETA = eta(v.Size, v.Downloaded, v.Speed)
	}
	return v
}

func eta(size, done, speed int64) int64 {
	if size <= 0 || speed <= 0 {
		return -1
	}
	return (size - done) / speed
}

// Downloads lists items for a queue (0 = all) and a status filter:
// "", "active", "unfinished" or "completed". Unfinished items come first in
// queue order, then completed ones newest first.
func (m *Manager) Downloads(queueID uint64, filter string) []DownloadView {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]DownloadView, 0, len(m.downloads))
	for _, d := range m.downloads {
		if queueID != 0 && d.QueueID != queueID {
			continue
		}
		_, active := m.tasks[d.ID]
		done := d.Status == store.StatusCompleted
		switch filter {
		case "active":
			if !active {
				continue
			}
		case "unfinished":
			if done {
				continue
			}
		case "completed":
			if !done {
				continue
			}
		}
		out = append(out, m.viewLocked(d))
	}
	slices.SortFunc(out, func(a, b DownloadView) int {
		ad, bd := a.Status == store.StatusCompleted, b.Status == store.StatusCompleted
		switch {
		case ad != bd:
			if ad {
				return 1
			}
			return -1
		case ad:
			return b.CompletedAt.Compare(a.CompletedAt)
		}
		return cmp.Or(cmp.Compare(a.Position, b.Position), cmp.Compare(a.ID, b.ID))
	})
	return out
}

func (m *Manager) Download(id uint64) (DownloadView, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.downloads[id]
	if d == nil {
		return DownloadView{}, false
	}
	return m.viewLocked(d), true
}

func (m *Manager) Queues() []QueueView {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]QueueView, 0, len(m.queues))
	for _, q := range m.queues {
		v := QueueView{Queue: *q}
		for _, d := range m.downloads {
			if d.QueueID != q.ID {
				continue
			}
			v.Total++
			if _, ok := m.tasks[d.ID]; ok {
				v.Active++
			} else if d.Status == store.StatusQueued {
				v.Pending++
			}
		}
		out = append(out, v)
	}
	slices.SortFunc(out, func(a, b QueueView) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

func (m *Manager) defaultQueueLocked() *store.Queue {
	var first *store.Queue
	for _, q := range m.queues {
		if q.Default {
			return q
		}
		if first == nil || q.ID < first.ID {
			first = q
		}
	}
	return first
}

// ---- downloads ----

func ValidURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

func (m *Manager) Add(req AddRequest) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.queues[req.QueueID]
	if q == nil {
		q = m.defaultQueueLocked()
	}
	conns := req.Connections
	if conns < 1 || conns > MaxConnections {
		conns = m.settings.DefaultConnections
	}
	batchDir, err := m.resolveDirLocked(req.BatchDir)
	if err != nil {
		return 0, err
	}
	pos := m.maxPositionLocked(q.ID)
	now := time.Now()
	var added []*store.Download
	for _, l := range req.Links {
		if !ValidURL(l.URL) {
			return 0, fmt.Errorf("invalid link: %s", l.URL)
		}
		dir, err := m.resolveDirLocked(l.Dir)
		if err != nil {
			return 0, err
		}
		if dir == "" {
			dir = batchDir
		}
		var mirrors []string
		for _, mu := range l.Mirrors {
			if !ValidURL(mu) {
				return 0, fmt.Errorf("invalid mirror link: %s", mu)
			}
			if mu != l.URL {
				mirrors = append(mirrors, mu)
			}
		}
		pos++
		added = append(added, &store.Download{
			URL: l.URL, Mirrors: mirrors, QueueID: q.ID, Position: pos,
			DirOverride: dir, Size: -1, Connections: conns,
			Status: store.StatusQueued, CreatedAt: now,
		})
	}
	if len(added) == 0 {
		return 0, errors.New("no links to add")
	}
	if err := m.st.SaveDownloads(added...); err != nil {
		return 0, err
	}
	for _, d := range added {
		m.downloads[d.ID] = d
	}
	if req.Start && !q.Running {
		q.Running = true
		m.st.SaveQueue(q)
	}
	m.scheduleLocked()
	m.refresh()
	return len(added), nil
}

func (m *Manager) maxPositionLocked(queueID uint64) int64 {
	var p int64
	for _, d := range m.downloads {
		if d.QueueID == queueID {
			p = max(p, d.Position)
		}
	}
	return p
}

func (m *Manager) Pause(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.downloads[id]
	if d == nil || d.Status == store.StatusCompleted {
		return
	}
	if t := m.tasks[id]; t != nil {
		t.stop(store.StatusPaused)
	}
	d.Status = store.StatusPaused
	m.st.SaveDownloads(d)
	m.refresh()
}

// Resume starts a download right away, regardless of its queue's state.
func (m *Manager) Resume(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.downloads[id]
	if d == nil || d.Status == store.StatusCompleted {
		return
	}
	if t := m.tasks[id]; t != nil {
		if t.ctx.Err() != nil { // still winding down from a pause
			t.restart = true
			d.Status = store.StatusDownloading
		}
		return
	}
	m.startLocked(d)
	m.refresh()
}

func (m *Manager) Delete(id uint64, fromDisk bool) error {
	m.mu.Lock()
	if t := m.tasks[id]; t != nil {
		t.stop(store.StatusPaused)
		m.mu.Unlock()
		<-t.doneCh
		m.mu.Lock()
	}
	defer m.mu.Unlock()
	d := m.downloads[id]
	if d == nil {
		return nil
	}
	delete(m.downloads, id)
	if err := m.st.DeleteDownload(id); err != nil {
		return err
	}
	var rmErr error
	if d.Dir != "" && d.FileName != "" {
		p := filepath.Join(d.Dir, d.FileName)
		if d.Status == store.StatusCompleted {
			if fromDisk {
				rmErr = removeIfExists(p)
			}
		} else {
			// Partial data is useless without its segment map.
			rmErr = removeIfExists(p + partSuffix)
		}
	}
	m.scheduleLocked()
	m.refresh()
	return rmErr
}

func removeIfExists(p string) error {
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (m *Manager) ClearCompleted(queueID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, d := range m.downloads {
		if d.Status == store.StatusCompleted && (queueID == 0 || d.QueueID == queueID) {
			delete(m.downloads, id)
			m.st.DeleteDownload(id)
		}
	}
	m.refresh()
}

func (m *Manager) Update(id uint64, req UpdateRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.downloads[id]
	if d == nil {
		return store.ErrNotFound
	}
	var mirrors []string
	for _, mu := range req.Mirrors {
		if !ValidURL(mu) {
			return fmt.Errorf("invalid mirror link: %s", mu)
		}
		if mu != d.URL && !slices.Contains(mirrors, mu) {
			mirrors = append(mirrors, mu)
		}
	}
	if req.Dir != nil || req.FileName != nil {
		if !m.editableTargetLocked(d) {
			return errors.New("folder and file name can only change before the download starts")
		}
		if req.Dir != nil {
			dir, err := m.resolveDirLocked(*req.Dir)
			if err != nil {
				return err
			}
			d.DirOverride = dir
		}
		if req.FileName != nil {
			d.FileName = sanitizeName(*req.FileName)
		}
		d.Dir, d.Category = "", "" // re-resolved on start
	}
	d.Mirrors = mirrors
	if req.Connections >= 1 && req.Connections <= MaxConnections {
		d.Connections = req.Connections
	}
	if q := m.queues[req.QueueID]; q != nil && q.ID != d.QueueID {
		d.QueueID = q.ID
		d.Position = m.maxPositionLocked(q.ID) + 1
		if t := m.tasks[id]; t != nil {
			t.lim.Store(m.limiters[q.ID])
		}
	}
	if err := m.st.SaveDownloads(d); err != nil {
		return err
	}
	m.scheduleLocked()
	m.refresh()
	return nil
}

func (m *Manager) editableTargetLocked(d *store.Download) bool {
	return m.tasks[d.ID] == nil && d.Status != store.StatusCompleted && d.Downloaded == 0 && len(d.Segments) == 0
}

func (m *Manager) CanEditTarget(id uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.downloads[id]
	return d != nil && m.editableTargetLocked(d)
}

// unfinishedLocked returns a queue's unfinished items in queue order.
func (m *Manager) unfinishedLocked(queueID uint64) []*store.Download {
	var list []*store.Download
	for _, d := range m.downloads {
		if d.QueueID == queueID && d.Status != store.StatusCompleted {
			list = append(list, d)
		}
	}
	slices.SortFunc(list, func(a, b *store.Download) int {
		return cmp.Or(cmp.Compare(a.Position, b.Position), cmp.Compare(a.ID, b.ID))
	})
	return list
}

func (m *Manager) renumberLocked(list []*store.Download) {
	for i, d := range list {
		d.Position = int64(i + 1)
	}
	m.st.SaveDownloads(list...)
}

// Move shifts an item within its queue: "top", "up", "down" or "bottom".
func (m *Manager) Move(id uint64, where string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.downloads[id]
	if d == nil {
		return
	}
	list := m.unfinishedLocked(d.QueueID)
	i := slices.Index(list, d)
	if i < 0 {
		return
	}
	list = slices.Delete(list, i, i+1)
	j := i
	switch where {
	case "top":
		j = 0
	case "up":
		j = max(i-1, 0)
	case "down":
		j = min(i+1, len(list))
	case "bottom":
		j = len(list)
	}
	list = slices.Insert(list, j, d)
	m.renumberLocked(list)
	m.refresh()
}

// Reorder applies a user-defined order (e.g. from drag and drop) to a queue.
// Items not mentioned keep their relative order after the listed ones.
func (m *Manager) Reorder(queueID uint64, ids []uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	list := m.unfinishedLocked(queueID)
	rank := make(map[uint64]int, len(ids))
	for i, id := range ids {
		rank[id] = i
	}
	slices.SortStableFunc(list, func(a, b *store.Download) int {
		ra, oka := rank[a.ID]
		rb, okb := rank[b.ID]
		switch {
		case oka && okb:
			return cmp.Compare(ra, rb)
		case oka:
			return -1
		case okb:
			return 1
		}
		return 0
	})
	m.renumberLocked(list)
	m.scheduleLocked()
	m.refresh()
}

// ---- queues ----

func (m *Manager) Queue(id uint64) (store.Queue, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.queues[id]
	if q == nil {
		return store.Queue{}, false
	}
	return *q, true
}

func (m *Manager) SaveQueue(in store.Queue) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" {
		return 0, errors.New("queue name is required")
	}
	if in.MaxConcurrent < 1 {
		in.MaxConcurrent = 1
	}
	in.SpeedLimit = max(in.SpeedLimit, 0)
	q := m.queues[in.ID]
	if q == nil {
		q = &store.Queue{}
	}
	q.Name, q.SpeedLimit, q.MaxConcurrent = in.Name, in.SpeedLimit, in.MaxConcurrent
	if in.Default && !q.Default {
		for _, o := range m.queues {
			if o.Default {
				o.Default = false
				m.st.SaveQueue(o)
			}
		}
		q.Default = true
	}
	if err := m.st.SaveQueue(q); err != nil {
		return 0, err
	}
	m.queues[q.ID] = q
	if l := m.limiters[q.ID]; l != nil {
		l.setRate(q.SpeedLimit)
	} else {
		m.limiters[q.ID] = newLimiter(q.SpeedLimit)
	}
	m.scheduleLocked()
	m.refresh()
	return q.ID, nil
}

func (m *Manager) DeleteQueue(id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.queues[id]
	if q == nil {
		return nil
	}
	if q.Builtin {
		return errors.New("built-in queues cannot be deleted")
	}
	delete(m.queues, id)
	if err := m.st.DeleteQueue(id); err != nil {
		return err
	}
	target := m.defaultQueueLocked()
	if q.Default {
		target.Default = true
		m.st.SaveQueue(target)
	}
	pos := m.maxPositionLocked(target.ID)
	var moved []*store.Download
	for _, d := range m.unfinishedLocked(id) {
		pos++
		d.QueueID, d.Position = target.ID, pos
		moved = append(moved, d)
	}
	for _, d := range m.downloads {
		if d.QueueID == id { // completed items
			d.QueueID = target.ID
			moved = append(moved, d)
		}
	}
	for _, d := range moved {
		if t := m.tasks[d.ID]; t != nil {
			t.lim.Store(m.limiters[target.ID])
		}
	}
	m.st.SaveDownloads(moved...)
	delete(m.limiters, id)
	m.scheduleLocked()
	m.refresh()
	return nil
}

func (m *Manager) StartQueue(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.queues[id]
	if q == nil {
		return
	}
	// Starting a queue also retries its failed and paused items.
	for _, d := range m.downloads {
		if d.QueueID == id && m.tasks[d.ID] == nil &&
			(d.Status == store.StatusError || d.Status == store.StatusPaused) {
			d.Status, d.Error = store.StatusQueued, ""
			m.st.SaveDownloads(d)
		}
	}
	q.Running = true
	m.st.SaveQueue(q)
	m.scheduleLocked()
	m.refresh()
}

func (m *Manager) StopQueue(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.queues[id]
	if q == nil {
		return
	}
	q.Running = false
	m.st.SaveQueue(q)
	for _, t := range m.tasks {
		if d := m.downloads[t.id]; d != nil && d.QueueID == id {
			t.stop(store.StatusQueued)
			d.Status = store.StatusQueued
		}
	}
	m.refresh()
}

// ---- scheduling ----

func (m *Manager) scheduleLocked() {
	if m.closed {
		return
	}
	active := map[uint64]int{}
	for _, t := range m.tasks {
		if d := m.downloads[t.id]; d != nil {
			active[d.QueueID]++
		}
	}
	for _, q := range m.queues {
		if !q.Running {
			continue
		}
		started := false
		for _, d := range m.unfinishedLocked(q.ID) {
			if active[q.ID] >= q.MaxConcurrent {
				started = true // still busy
				break
			}
			if d.Status == store.StatusQueued && m.tasks[d.ID] == nil {
				m.startLocked(d)
				active[q.ID]++
				started = true
			}
		}
		if !started && active[q.ID] == 0 {
			// Queue finished: stop it so new links ask before starting.
			q.Running = false
			m.st.SaveQueue(q)
		}
	}
}

func (m *Manager) startLocked(d *store.Download) {
	ctx, cancel := context.WithCancel(context.Background())
	t := &task{id: d.ID, m: m, ctx: ctx, cancel: cancel, doneCh: make(chan struct{}), started: time.Now()}
	t.lim.Store(m.limiters[d.QueueID])
	t.downloaded.Store(d.Downloaded)
	m.tasks[d.ID] = t
	d.Status, d.Error = store.StatusDownloading, ""
	m.st.SaveDownloads(d)
	conns := d.Connections
	if conns < 1 {
		conns = m.settings.DefaultConnections
	}
	go t.run(*d, conns)
}

// prepare settles the target folder and file name the first time a download
// starts, once the server has told us the real file name.
func (m *Manager) prepare(id uint64, ref *probeResult) (store.Download, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.downloads[id]
	if d == nil {
		return store.Download{}, store.ErrNotFound
	}
	if d.Dir == "" {
		if d.FileName == "" {
			d.FileName = ref.name
		}
		d.Category = categorize(m.settings.Categories, d.FileName)
		switch {
		case d.DirOverride != "":
			d.Dir = d.DirOverride
		case m.settings.Organize:
			d.Dir = filepath.Join(m.settings.BaseDir, d.Category)
		default:
			d.Dir = m.settings.BaseDir
		}
		dir := d.Dir
		d.FileName = uniqueName(d.FileName, func(c string) bool {
			if exists(filepath.Join(dir, c)) || exists(filepath.Join(dir, c+partSuffix)) {
				return true
			}
			for _, o := range m.downloads {
				if o.ID != id && o.Dir == dir && o.FileName == c {
					return true
				}
			}
			return false
		})
		m.st.SaveDownloads(d)
	}
	return *d, nil
}

func (m *Manager) updateMeta(id uint64, ref *probeResult, segs []store.Segment) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.downloads[id]
	if d == nil {
		return
	}
	d.Size, d.Resumable, d.Validator, d.Segments = ref.size, ref.resumable, ref.validator, segs
	m.st.SaveDownloads(d)
	m.refresh()
}

func (m *Manager) finish(t *task, res result) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, t.id)
	d := m.downloads[t.id]
	if d == nil {
		return
	}
	segs, resumable := t.state()
	switch {
	case res.completed:
		d.Status, d.Error = store.StatusCompleted, ""
		d.FileName = res.finalName
		d.Downloaded = t.downloaded.Load()
		if d.Size < 0 {
			d.Size = d.Downloaded
		}
		d.Segments = nil
		d.CompletedAt = time.Now()
	case t.ctx.Err() != nil:
		if s, ok := t.stopAs.Load().(store.Status); ok {
			d.Status = s
		} else {
			d.Status = store.StatusPaused
		}
		d.Segments = segs
		d.Downloaded = t.downloaded.Load()
		if !resumable {
			d.Downloaded = 0
		}
	default:
		d.Status = store.StatusError
		d.Error = res.err.Error()
		d.Segments = segs
		d.Downloaded = t.downloaded.Load()
		if !resumable {
			d.Downloaded = 0
		}
	}
	if t.restart && d.Status != store.StatusCompleted && !m.closed {
		m.startLocked(d)
	}
	m.st.SaveDownloads(d)
	m.scheduleLocked()
	m.refresh()
}

func (m *Manager) ticker() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for n := 1; ; n++ {
		<-tick.C
		m.mu.Lock()
		if len(m.tasks) == 0 {
			m.mu.Unlock()
			continue
		}
		ev := ProgressEvent{Items: make([]Progress, 0, len(m.tasks))}
		var dirty []*store.Download
		for id, t := range m.tasks {
			d := m.downloads[id]
			if d == nil {
				continue
			}
			cur := t.downloaded.Load()
			inst := max(cur-t.lastSample, 0)
			t.lastSample = cur
			sp := t.speed.Load()
			if sp > 0 {
				sp = (sp*6 + inst*4) / 10 // smooth the displayed speed
			} else {
				sp = inst
			}
			t.speed.Store(sp)
			ev.Total += sp
			ev.Items = append(ev.Items, Progress{ID: id, Done: cur, Size: d.Size, Speed: sp, ETA: eta(d.Size, cur, sp)})
			if n%saveEvery == 0 {
				segs, _ := t.state()
				d.Downloaded, d.Segments = cur, segs
				dirty = append(dirty, d)
			}
		}
		if len(dirty) > 0 {
			m.st.SaveDownloads(dirty...)
		}
		m.mu.Unlock()
		m.publish(Event{Kind: "progress", Data: ev})
	}
}

// ---- files ----

func exists(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}

func joinPath(dir, name string) string { return filepath.Join(dir, name) }

// uniqueName appends " (n)" before the extension until taken reports false.
func uniqueName(name string, taken func(string) bool) string {
	ext := filepath.Ext(name)
	stem := strings.TrimSuffix(name, ext)
	if strings.HasSuffix(strings.ToLower(stem), ".tar") {
		ext = stem[len(stem)-4:] + ext
		stem = stem[:len(stem)-4]
	}
	cand := name
	for i := 1; taken(cand); i++ {
		cand = fmt.Sprintf("%s (%d)%s", stem, i, ext)
	}
	return cand
}
