package web

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"idm/internal/store"
)

const (
	cookieName     = "idm_session"
	sessionTTL     = 30 * 24 * time.Hour
	pbkdf2Rounds   = 600_000
	failedLoginLag = time.Second
)

// SetPassword stores a salted PBKDF2 hash of pw in s.
func SetPassword(s *store.Settings, pw string) error {
	salt := make([]byte, 16)
	rand.Read(salt)
	hash, err := pbkdf2.Key(sha256.New, pw, salt, pbkdf2Rounds, 32)
	if err != nil {
		return err
	}
	s.PasswordSalt, s.PasswordHash = salt, hash
	return nil
}

func CheckPassword(s store.Settings, pw string) bool {
	if len(s.PasswordHash) == 0 {
		return false
	}
	hash, err := pbkdf2.Key(sha256.New, pw, s.PasswordSalt, pbkdf2Rounds, 32)
	return err == nil && subtle.ConstantTimeCompare(hash, s.PasswordHash) == 1
}

// RandomPassword returns a human-typeable random password.
func RandomPassword() string {
	return strings.ToLower(rand.Text()[:16])
}

func sign(secret []byte, msg string) string {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(msg))
	return hex.EncodeToString(h.Sum(nil))
}

// Sessions are stateless: "<expiry>.<hmac>", signed with a secret that is
// rotated whenever the password changes, which logs everyone out.
func (s *Server) newSession(w http.ResponseWriter, r *http.Request) {
	exp := strconv.FormatInt(time.Now().Add(sessionTTL).Unix(), 10)
	http.SetCookie(w, &http.Cookie{
		Name: cookieName, Value: exp + "." + sign(s.m.Settings().SessionSecret, exp),
		Path: "/", HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode,
		MaxAge: int(sessionTTL.Seconds()),
	})
}

func (s *Server) validSession(r *http.Request) bool {
	c, err := r.Cookie(cookieName)
	if err != nil {
		return false
	}
	exp, mac, ok := strings.Cut(c.Value, ".")
	if !ok {
		return false
	}
	n, err := strconv.ParseInt(exp, 10, 64)
	if err != nil || time.Now().Unix() > n {
		return false
	}
	return hmac.Equal([]byte(mac), []byte(sign(s.m.Settings().SessionSecret, exp)))
}

// sameOrigin rejects cross-site form posts (defense in depth on top of
// SameSite=Strict cookies).
func sameOrigin(r *http.Request) bool {
	src := r.Header.Get("Origin")
	if src == "" {
		src = r.Header.Get("Referer")
	}
	if src == "" {
		return true
	}
	u, err := url.Parse(src)
	return err == nil && u.Host == r.Host
}

func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead && !sameOrigin(r) {
			http.Error(w, "cross-origin request rejected", http.StatusForbidden)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/static/") || r.URL.Path == "/login" || s.validSession(r) {
			next.ServeHTTP(w, r)
			return
		}
		if r.Header.Get("HX-Request") != "" {
			w.Header().Set("HX-Redirect", "/login")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		http.Redirect(w, r, "/login", http.StatusSeeOther)
	})
}

var loginMu sync.Mutex // serializes attempts so failures can't be parallelized

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		s.render(w, "login", map[string]any{})
		return
	}
	loginMu.Lock()
	defer loginMu.Unlock()
	if !CheckPassword(s.m.Settings(), r.PostFormValue("password")) {
		time.Sleep(failedLoginLag)
		w.WriteHeader(http.StatusUnauthorized)
		s.render(w, "login", map[string]any{"Error": "Wrong password"})
		return
	}
	s.newSession(w, r)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: "", Path: "/", MaxAge: -1})
	w.Header().Set("HX-Redirect", "/login")
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}
