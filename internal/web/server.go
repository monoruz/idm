// Package web serves the htmx UI and the SSE progress stream.
package web

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"idm/internal/engine"
	"idm/internal/store"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static
var staticFS embed.FS

type Server struct {
	m    *engine.Manager
	tmpl *template.Template
}

func New(m *engine.Manager) (*Server, error) {
	// Content hash in asset URLs lets browsers cache them across restarts
	// without serving stale files after an upgrade.
	h := sha256.New()
	fs.WalkDir(staticFS, "static", func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			b, _ := staticFS.ReadFile(p)
			h.Write(b)
		}
		return nil
	})
	version := hex.EncodeToString(h.Sum(nil))[:10]
	funcs["asset"] = func(name string) string { return "/static/" + name + "?v=" + version }
	t, err := template.New("").Funcs(funcs).ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, err
	}
	return &Server{m: m, tmpl: t}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	static, _ := fs.Sub(staticFS, "static")
	mux.Handle("GET /static/", http.StripPrefix("/static/", cacheFor(http.FileServerFS(static))))
	mux.HandleFunc("/login", s.login)
	mux.HandleFunc("POST /logout", s.logout)

	mux.HandleFunc("GET /{$}", s.index)
	mux.HandleFunc("GET /view", s.view)
	mux.HandleFunc("GET /events", s.events)

	mux.HandleFunc("GET /add", s.addForm)
	mux.HandleFunc("POST /add/preview", s.addPreview)
	mux.HandleFunc("POST /add", s.add)

	mux.HandleFunc("GET /d/{id}", s.details)
	mux.HandleFunc("POST /d/{id}", s.update)
	mux.HandleFunc("POST /d/{id}/pause", s.itemAction(func(id uint64) { s.m.Pause(id) }))
	mux.HandleFunc("POST /d/{id}/resume", s.itemAction(func(id uint64) { s.m.Resume(id) }))
	mux.HandleFunc("POST /d/{id}/move", s.move)
	mux.HandleFunc("GET /d/{id}/delete", s.deleteForm)
	mux.HandleFunc("POST /d/{id}/delete", s.delete)
	mux.HandleFunc("POST /clear", s.clear)

	mux.HandleFunc("GET /queues/new", s.queueForm)
	mux.HandleFunc("GET /queues/{id}", s.queueForm)
	mux.HandleFunc("POST /queues", s.saveQueue)
	mux.HandleFunc("POST /queues/{id}/delete", s.queueAction(s.m.DeleteQueue))
	mux.HandleFunc("POST /queues/{id}/start", s.queueAction(func(id uint64) error { s.m.StartQueue(id); return nil }))
	mux.HandleFunc("POST /queues/{id}/stop", s.queueAction(func(id uint64) error { s.m.StopQueue(id); return nil }))
	mux.HandleFunc("POST /queues/{id}/order", s.reorder)

	mux.HandleFunc("GET /settings", s.settingsForm)
	mux.HandleFunc("POST /settings", s.saveSettings)
	return s.requireAuth(mux)
}

func cacheFor(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		h.ServeHTTP(w, r)
	})
}

// ---- rendering helpers ----

func (s *Server) render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.ExecuteTemplate(w, name, data); err != nil {
		log.Printf("render %s: %v", name, err)
	}
}

// fail shows msg in the toast area without disturbing the current view.
func fail(w http.ResponseWriter, msg string) {
	w.Header().Set("HX-Retarget", "#toast")
	w.Header().Set("HX-Reswap", "innerHTML")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<div class="toast err">%s</div>`, template.HTMLEscapeString(msg))
}

// done closes any dialog and asks the page to refresh its view.
func done(w http.ResponseWriter) {
	w.Header().Set("HX-Trigger", `{"refresh":true,"closeDialog":true}`)
	w.WriteHeader(http.StatusNoContent)
}

func pathID(r *http.Request) uint64 {
	id, _ := strconv.ParseUint(r.PathValue("id"), 10, 64)
	return id
}

func formID(r *http.Request, key string) uint64 {
	id, _ := strconv.ParseUint(r.FormValue(key), 10, 64)
	return id
}

// ---- views ----

type viewData struct {
	Queues  []engine.QueueView
	Items   []engine.DownloadView
	Sel     uint64 // selected queue, 0 = all
	Queue   *engine.QueueView
	Filter  string
	Title   string
	OOB     bool
	Version string
}

var filters = map[string]string{"": "All downloads", "active": "Downloading", "unfinished": "Unfinished", "completed": "Completed"}

func (s *Server) viewData(r *http.Request) viewData {
	d := viewData{Sel: formID(r, "q"), Filter: r.FormValue("f"), Queues: s.m.Queues()}
	if _, ok := filters[d.Filter]; !ok {
		d.Filter = ""
	}
	d.Title = filters[d.Filter]
	if d.Sel != 0 {
		d.Filter = ""
		for i := range d.Queues {
			if d.Queues[i].ID == d.Sel {
				d.Queue = &d.Queues[i]
				d.Title = d.Queue.Name
			}
		}
		if d.Queue == nil {
			d.Sel = 0
		}
	}
	d.Items = s.m.Downloads(d.Sel, d.Filter)
	return d
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	s.render(w, "page", s.viewData(r))
}

func (s *Server) view(w http.ResponseWriter, r *http.Request) {
	d := s.viewData(r)
	d.OOB = true
	s.render(w, "view", d)
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	ch, cancel := s.m.Subscribe()
	defer cancel()
	fmt.Fprint(w, "retry: 2000\n\n")
	fl.Flush()
	ping := time.NewTicker(25 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
		case ev := <-ch:
			data := []byte("1")
			if ev.Data != nil {
				data, _ = json.Marshal(ev.Data)
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Kind, data)
		}
		fl.Flush()
	}
}

// ---- adding links ----

var urlRe = regexp.MustCompile(`https?://[^\s"'<>]+`)

type previewLink struct {
	URL     string
	Mirrors string
}

// extractLinks finds links in free text. A line of the form
// "url | mirror | mirror" yields one link with mirrors.
func extractLinks(text string) []previewLink {
	var out []previewLink
	seen := map[string]bool{}
	for _, line := range strings.Split(text, "\n") {
		urls := urlRe.FindAllString(line, -1)
		for i := range urls {
			urls[i] = strings.TrimRight(urls[i], ".,;:)]}|")
		}
		if len(urls) == 0 {
			continue
		}
		if strings.Contains(line, "|") && len(urls) > 1 {
			if !seen[urls[0]] {
				seen[urls[0]] = true
				out = append(out, previewLink{URL: urls[0], Mirrors: strings.Join(urls[1:], " ")})
			}
			continue
		}
		for _, u := range urls {
			if !seen[u] && engine.ValidURL(u) {
				seen[u] = true
				out = append(out, previewLink{URL: u})
			}
		}
	}
	return out
}

func (s *Server) addForm(w http.ResponseWriter, r *http.Request) {
	st := s.m.Settings()
	s.render(w, "add", map[string]any{
		"Queues": s.m.Queues(), "Sel": formID(r, "q"), "Conns": st.DefaultConnections,
		"BaseDir": st.BaseDir, "Max": engine.MaxConnections,
	})
}

func (s *Server) addPreview(w http.ResponseWriter, r *http.Request) {
	s.render(w, "preview", extractLinks(r.FormValue("text")))
}

func (s *Server) add(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	urls, dirs, mirrors := r.Form["url"], r.Form["dir"], r.Form["mirrors"]
	if len(dirs) != len(urls) || len(mirrors) != len(urls) {
		fail(w, "Malformed form")
		return
	}
	req := engine.AddRequest{
		QueueID:     formID(r, "queue"),
		BatchDir:    r.FormValue("batch"),
		Start:       r.FormValue("start") != "",
		Connections: atoi(r.FormValue("conns")),
	}
	for _, v := range r.Form["inc"] {
		i := atoi(v)
		if i < 0 || i >= len(urls) {
			continue
		}
		req.Links = append(req.Links, engine.AddLink{URL: urls[i], Dir: dirs[i], Mirrors: strings.Fields(mirrors[i])})
	}
	if len(req.Links) == 0 {
		fail(w, "Select at least one link")
		return
	}
	if q, ok := s.m.Queue(req.QueueID); ok && q.Running {
		req.Start = true // queue is already downloading; just append
	}
	if _, err := s.m.Add(req); err != nil {
		fail(w, err.Error())
		return
	}
	done(w)
}

func atoi(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}

// ---- single download ----

func (s *Server) details(w http.ResponseWriter, r *http.Request) {
	d, ok := s.m.Download(pathID(r))
	if !ok {
		fail(w, "Download not found")
		return
	}
	s.render(w, "details", map[string]any{
		"D": d, "Queues": s.m.Queues(), "Editable": s.m.CanEditTarget(d.ID), "Max": engine.MaxConnections,
	})
}

func (s *Server) update(w http.ResponseWriter, r *http.Request) {
	req := engine.UpdateRequest{
		Mirrors:     strings.Fields(r.FormValue("mirrors")),
		Connections: atoi(r.FormValue("conns")),
		QueueID:     formID(r, "queue"),
	}
	if r.Form.Has("dir") {
		v := r.FormValue("dir")
		req.Dir = &v
	}
	if r.Form.Has("name") {
		v := r.FormValue("name")
		req.FileName = &v
	}
	if err := s.m.Update(pathID(r), req); err != nil {
		fail(w, err.Error())
		return
	}
	done(w)
}

func (s *Server) itemAction(fn func(uint64)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fn(pathID(r))
		done(w)
	}
}

func (s *Server) move(w http.ResponseWriter, r *http.Request) {
	s.m.Move(pathID(r), r.FormValue("to"))
	done(w)
}

func (s *Server) deleteForm(w http.ResponseWriter, r *http.Request) {
	d, ok := s.m.Download(pathID(r))
	if !ok {
		fail(w, "Download not found")
		return
	}
	s.render(w, "delete", d)
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	if err := s.m.Delete(pathID(r), r.FormValue("disk") != ""); err != nil {
		fail(w, "Removed from list, but deleting the file failed: "+err.Error())
		return
	}
	done(w)
}

func (s *Server) clear(w http.ResponseWriter, r *http.Request) {
	s.m.ClearCompleted(formID(r, "q"))
	done(w)
}

// ---- queues ----

func (s *Server) queueForm(w http.ResponseWriter, r *http.Request) {
	q := store.Queue{MaxConcurrent: 1}
	if id := pathID(r); id != 0 {
		var ok bool
		if q, ok = s.m.Queue(id); !ok {
			fail(w, "Queue not found")
			return
		}
	}
	s.render(w, "queue", q)
}

func (s *Server) saveQueue(w http.ResponseWriter, r *http.Request) {
	q := store.Queue{
		ID:            formID(r, "id"),
		Name:          r.FormValue("name"),
		SpeedLimit:    int64(atoi(r.FormValue("limit"))) << 10,
		MaxConcurrent: atoi(r.FormValue("max")),
		Default:       r.FormValue("default") != "",
	}
	if _, err := s.m.SaveQueue(q); err != nil {
		fail(w, err.Error())
		return
	}
	done(w)
}

func (s *Server) queueAction(fn func(uint64) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(pathID(r)); err != nil {
			fail(w, err.Error())
			return
		}
		done(w)
	}
}

func (s *Server) reorder(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	var ids []uint64
	for _, v := range r.Form["ids"] {
		if id, err := strconv.ParseUint(v, 10, 64); err == nil {
			ids = append(ids, id)
		}
	}
	s.m.Reorder(pathID(r), ids)
	done(w)
}

// ---- settings ----

func (s *Server) settingsForm(w http.ResponseWriter, r *http.Request) {
	st := s.m.Settings()
	var cats strings.Builder
	for _, c := range st.Categories {
		fmt.Fprintf(&cats, "%s: %s\n", c.Name, strings.Join(c.Extensions, " "))
	}
	s.render(w, "settings", map[string]any{"S": st, "Cats": cats.String(), "Max": engine.MaxConnections})
}

func parseCategories(text string) ([]store.Category, error) {
	var out []store.Category
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		name, exts, ok := strings.Cut(line, ":")
		name = strings.TrimSpace(name)
		if !ok || name == "" || strings.ContainsAny(name, `/\`) || name == "." || name == ".." {
			return nil, fmt.Errorf("invalid category line %q (expected \"Name: ext ext\")", line)
		}
		c := store.Category{Name: name}
		for _, e := range strings.Fields(strings.ToLower(exts)) {
			e = strings.TrimPrefix(e, ".")
			if e != "" && !slices.Contains(c.Extensions, e) {
				c.Extensions = append(c.Extensions, e)
			}
		}
		out = append(out, c)
	}
	return out, nil
}

func (s *Server) saveSettings(w http.ResponseWriter, r *http.Request) {
	cats, err := parseCategories(r.FormValue("categories"))
	if err != nil {
		fail(w, err.Error())
		return
	}
	newPw := r.FormValue("newpw")
	err = s.m.UpdateSettings(func(st *store.Settings) error {
		dir := strings.TrimSpace(r.FormValue("basedir"))
		if !filepath.IsAbs(dir) {
			return fmt.Errorf("download folder must be an absolute path")
		}
		st.BaseDir = dir
		st.Organize = r.FormValue("organize") != ""
		st.Categories = cats
		if c := atoi(r.FormValue("conns")); c >= 1 && c <= engine.MaxConnections {
			st.DefaultConnections = c
		}
		if newPw != "" {
			if !CheckPassword(*st, r.FormValue("curpw")) {
				return fmt.Errorf("current password is wrong")
			}
			if len(newPw) < 8 {
				return fmt.Errorf("new password must be at least 8 characters")
			}
			if err := SetPassword(st, newPw); err != nil {
				return err
			}
			st.SessionSecret = randomBytes(32)
		}
		return nil
	})
	if err != nil {
		fail(w, err.Error())
		return
	}
	if newPw != "" {
		s.newSession(w, r) // keep this browser logged in with the new secret
	}
	done(w)
}
