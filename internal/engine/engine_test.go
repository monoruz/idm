package engine

import (
	"bytes"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"idm/internal/store"
)

func newTestManager(t *testing.T) (*Manager, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	dl := filepath.Join(dir, "dl")
	m, err := New(st, &store.Settings{BaseDir: dl, Organize: true, Categories: DefaultCategories(), DefaultConnections: 8}, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close(); st.Close() })
	return m, dl
}

func randomData(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// fileServer serves data with range support; hook may veto a request.
func fileServer(data []byte, ranges bool, hook func(r *http.Request) bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hook != nil && !hook(r) {
			http.Error(w, "nope", http.StatusInternalServerError)
			return
		}
		if !ranges {
			r.Header.Del("Range")
			w.Header().Set("Accept-Ranges", "none")
			w.Write(data)
			return
		}
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
	}))
}

func waitStatus(t *testing.T, m *Manager, id uint64, want store.Status, timeout time.Duration) DownloadView {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		d, _ := m.Download(id)
		if d.Status == want && !d.Active {
			return d
		}
		if d.Status == store.StatusError && want != store.StatusError {
			t.Fatalf("download failed: %s", d.Error)
		}
		time.Sleep(20 * time.Millisecond)
	}
	d, _ := m.Download(id)
	t.Fatalf("timeout waiting for %s; status=%s active=%v err=%s", want, d.Status, d.Active, d.Error)
	return d
}

func onlyID(t *testing.T, m *Manager) uint64 {
	t.Helper()
	items := m.Downloads(0, "")
	if len(items) != 1 {
		t.Fatalf("want 1 item, got %d", len(items))
	}
	return items[0].ID
}

func checkFile(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(want))
	}
}

func highQueue(m *Manager) uint64 { return m.Queues()[0].ID }
func lowQueue(m *Manager) uint64  { return m.Queues()[1].ID }

func TestMultiConnectionWithMirror(t *testing.T) {
	data := randomData(6 << 20)
	var primaryHits, mirrorHits atomic.Int32
	primary := fileServer(data, true, func(*http.Request) bool { primaryHits.Add(1); return true })
	defer primary.Close()
	mirror := fileServer(data, true, func(*http.Request) bool { mirrorHits.Add(1); return true })
	defer mirror.Close()

	m, dl := newTestManager(t)
	_, err := m.Add(AddRequest{QueueID: highQueue(m), Start: true, Links: []AddLink{{
		URL: primary.URL + "/movie.mkv", Mirrors: []string{mirror.URL + "/movie.mkv"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	d := waitStatus(t, m, onlyID(t, m), store.StatusCompleted, 20*time.Second)
	checkFile(t, filepath.Join(dl, "Video", "movie.mkv"), data)
	if d.Category != "Video" || d.Size != int64(len(data)) {
		t.Fatalf("unexpected meta: %+v", d.Download)
	}
	if mirrorHits.Load() < 2 {
		t.Fatalf("mirror was not used for segments (hits=%d)", mirrorHits.Load())
	}
	if q, _ := m.Queue(highQueue(m)); q.Running {
		t.Fatal("queue should stop once it has nothing left")
	}
}

func TestFailoverToMirror(t *testing.T) {
	data := randomData(3 << 20)
	var n atomic.Int32
	// Primary answers the probe, then fails every ranged request.
	primary := fileServer(data, true, func(r *http.Request) bool {
		return n.Add(1) == 1
	})
	defer primary.Close()
	mirror := fileServer(data, true, nil)
	defer mirror.Close()

	m, dl := newTestManager(t)
	m.Add(AddRequest{QueueID: highQueue(m), Start: true, Links: []AddLink{{
		URL: primary.URL + "/a.zip", Mirrors: []string{mirror.URL + "/a.zip"},
	}}})
	waitStatus(t, m, onlyID(t, m), store.StatusCompleted, 30*time.Second)
	checkFile(t, filepath.Join(dl, "Compressed", "a.zip"), data)
}

func TestPauseResumeAndSpeedLimit(t *testing.T) {
	data := randomData(3 << 20)
	srv := fileServer(data, true, nil)
	defer srv.Close()

	m, dl := newTestManager(t)
	m.SaveQueue(store.Queue{ID: lowQueue(m), Name: "Low", SpeedLimit: 1 << 20, MaxConcurrent: 1})
	start := time.Now()
	m.Add(AddRequest{QueueID: lowQueue(m), Start: true, Links: []AddLink{{URL: srv.URL + "/song.mp3"}}})
	id := onlyID(t, m)

	time.Sleep(1200 * time.Millisecond)
	m.Pause(id)
	d := waitStatus(t, m, id, store.StatusPaused, 5*time.Second)
	if d.Downloaded <= 0 || d.Downloaded >= int64(len(data)) {
		t.Fatalf("paused at %d bytes", d.Downloaded)
	}
	// ~1.2 MiB at 1 MiB/s, allowing for the initial burst.
	if d.Downloaded > 2<<20 {
		t.Fatalf("speed limit not enforced: %d bytes in 1.2s", d.Downloaded)
	}
	if _, err := os.Stat(filepath.Join(dl, "Music", "song.mp3"+partSuffix)); err != nil {
		t.Fatal("partial file missing:", err)
	}

	m.Resume(id)
	waitStatus(t, m, id, store.StatusCompleted, 15*time.Second)
	checkFile(t, filepath.Join(dl, "Music", "song.mp3"), data)
	if el := time.Since(start); el < 2500*time.Millisecond {
		t.Fatalf("3 MiB at 1 MiB/s finished in %v", el)
	}
}

func TestNoRangeSupport(t *testing.T) {
	data := randomData(1 << 20)
	srv := fileServer(data, false, nil)
	defer srv.Close()
	m, dl := newTestManager(t)
	m.Add(AddRequest{QueueID: highQueue(m), Start: true, Links: []AddLink{{URL: srv.URL + "/doc.pdf"}}})
	d := waitStatus(t, m, onlyID(t, m), store.StatusCompleted, 10*time.Second)
	if d.Resumable {
		t.Fatal("should not be resumable")
	}
	checkFile(t, filepath.Join(dl, "Documents", "doc.pdf"), data)
}

func TestQueueConcurrencyOrderAndDelete(t *testing.T) {
	data := randomData(2 << 20)
	srv := fileServer(data, true, nil)
	defer srv.Close()
	m, dl := newTestManager(t)
	q := lowQueue(m)
	m.SaveQueue(store.Queue{ID: q, Name: "Low", SpeedLimit: 512 << 10, MaxConcurrent: 1})
	m.Add(AddRequest{QueueID: q, BatchDir: "batch", Links: []AddLink{
		{URL: srv.URL + "/1.bin"}, {URL: srv.URL + "/2.bin"}, {URL: srv.URL + "/3.bin", Dir: "custom"},
	}})
	items := m.Downloads(q, "")
	// Move the last one to the top before starting.
	m.Move(items[2].ID, "top")
	m.StartQueue(q)
	time.Sleep(300 * time.Millisecond)
	active := m.Downloads(q, "active")
	if len(active) != 1 || active[0].ID != items[2].ID {
		t.Fatalf("expected only the moved item active, got %+v", active)
	}
	if _, err := m.ResolveDir("/etc"); err == nil {
		t.Fatal("folders outside the download folder must be rejected")
	}

	m.StopQueue(q)
	waitStatus(t, m, items[2].ID, store.StatusQueued, 5*time.Second)
	for _, it := range m.Downloads(q, "") {
		m.Delete(it.ID, true)
	}
	if n := len(m.Downloads(0, "")); n != 0 {
		t.Fatalf("%d items left", n)
	}
	if _, err := os.Stat(filepath.Join(dl, "custom", "3.bin"+partSuffix)); !os.IsNotExist(err) {
		t.Fatal("partial file should be removed on delete")
	}
}

func TestUniqueName(t *testing.T) {
	taken := map[string]bool{"a.tar.gz": true, "a (1).tar.gz": true, "b.txt": true}
	if got := uniqueName("a.tar.gz", func(s string) bool { return taken[s] }); got != "a (2).tar.gz" {
		t.Fatal(got)
	}
	if got := uniqueName("b.txt", func(s string) bool { return taken[s] }); got != "b (1).txt" {
		t.Fatal(got)
	}
	if !strings.HasPrefix(sanitizeName("../../etc/passwd"), "_") {
		t.Fatal("path separators must be sanitized")
	}
}
