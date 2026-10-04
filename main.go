// Command idm is a lightweight download manager with a web UI.
//
//	idm [flags]        run the server
//	idm init [flags]   create the database and save the UI password, then exit
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"

	"idm/internal/engine"
	"idm/internal/store"
	"idm/internal/web"
)

const minPasswordLen = 8

func main() {
	if len(os.Args) > 1 && os.Args[1] == "init" {
		if err := runInit(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "idm init:", err)
			os.Exit(1)
		}
		return
	}
	runServer()
}

func defaultDirs() (data, downloads string) {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".idm"), filepath.Join(home, "Downloads", "idm")
}

func openStore(dataDir string) (*store.Store, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	return store.Open(filepath.Join(dataDir, "idm.db"))
}

// runInit saves the UI password into the database without starting the
// server. The password is read from the terminal (hidden), or from a single
// line on stdin when piped.
func runInit(args []string) error {
	dataDef, dlDef := defaultDirs()
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	dataDir := fs.String("data", dataDef, "folder for the database")
	dlDir := fs.String("dir", dlDef, "download folder, used when the database is new")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: idm init [-data DIR] [-dir DIR]\n\nSaves the UI password in the database. Stop the server first.")
		fs.PrintDefaults()
	}
	fs.Parse(args)

	pw, err := readPassword()
	if err != nil {
		return err
	}
	st, err := openStore(*dataDir)
	if err != nil {
		return err
	}
	defer st.Close()
	s, err := loadSettings(st, *dlDir)
	if err != nil {
		return err
	}
	if err := web.SetPassword(s, pw); err != nil {
		return err
	}
	s.SessionSecret = web.RandomSecret() // log out existing sessions
	if err := st.SaveSettings(s); err != nil {
		return err
	}
	fmt.Printf("password saved to %s\n", filepath.Join(*dataDir, "idm.db"))
	return nil
}

func readPassword() (string, error) {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil && line == "" {
			return "", errors.New("no password on stdin")
		}
		pw := strings.TrimRight(line, "\r\n")
		if len(pw) < minPasswordLen {
			return "", fmt.Errorf("password must be at least %d characters", minPasswordLen)
		}
		return pw, nil
	}
	for {
		fmt.Fprint(os.Stderr, "New password: ")
		a, err := term.ReadPassword(fd)
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", err
		}
		if len(a) < minPasswordLen {
			fmt.Fprintf(os.Stderr, "Must be at least %d characters.\n", minPasswordLen)
			continue
		}
		fmt.Fprint(os.Stderr, "Repeat password: ")
		b, err := term.ReadPassword(fd)
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", err
		}
		if string(a) != string(b) {
			fmt.Fprintln(os.Stderr, "Passwords do not match.")
			continue
		}
		return string(a), nil
	}
}

func runServer() {
	dataDef, dlDef := defaultDirs()
	addr := flag.String("addr", "0.0.0.0:8080", "listen address")
	dataDir := flag.String("data", dataDef, "folder for the database")
	dlDir := flag.String("dir", dlDef, "initial download folder (changeable in settings)")
	anyDir := flag.Bool("allow-any-dir", false, "allow saving outside the download folder")
	memLimit := flag.Int64("mem-limit", 48, "soft memory limit in MiB (0 = Go default)")
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "Usage: idm [flags]\n       idm init [-data DIR] [-dir DIR]   set the UI password\n\nFlags:")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *memLimit > 0 {
		debug.SetMemoryLimit(*memLimit << 20)
	}
	st, err := openStore(*dataDir)
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	settings, err := loadSettings(st, *dlDir)
	if err != nil {
		log.Fatal(err)
	}
	if len(settings.PasswordHash) == 0 {
		pw := web.RandomPassword()
		if err := web.SetPassword(settings, pw); err != nil {
			log.Fatal(err)
		}
		log.Printf("no password set; generated: %s  (change it in Settings or with `idm init`)", pw)
	}
	if err := st.SaveSettings(settings); err != nil {
		log.Fatal(err)
	}
	m, err := engine.New(st, settings, *anyDir)
	if err != nil {
		log.Fatal(err)
	}
	srv, err := web.New(m)
	if err != nil {
		log.Fatal(err)
	}

	// Cancelled on shutdown so long-lived SSE streams end promptly.
	baseCtx, cancelBase := context.WithCancel(context.Background())
	hs := &http.Server{
		Addr:              *addr,
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		log.Printf("listening on http://%s (downloads: %s)", *addr, settings.BaseDir)
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Print("shutting down")
	cancelBase()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hs.Shutdown(ctx)
	m.Close()
}

// loadSettings returns stored settings, or defaults on first run, and makes
// sure the download folder exists.
func loadSettings(st *store.Store, dlDir string) (*store.Settings, error) {
	s, err := st.Settings()
	switch {
	case errors.Is(err, store.ErrNotFound):
		abs, err := filepath.Abs(dlDir)
		if err != nil {
			return nil, err
		}
		s = &store.Settings{
			BaseDir:            abs,
			Organize:           true,
			Categories:         engine.DefaultCategories(),
			DefaultConnections: 8,
		}
	case err != nil:
		return nil, err
	}
	if len(s.SessionSecret) == 0 {
		s.SessionSecret = web.RandomSecret()
	}
	if err := os.MkdirAll(s.BaseDir, 0o755); err != nil {
		return nil, err
	}
	return s, nil
}
