package engine

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

type probeResult struct {
	url       string
	size      int64 // -1 unknown
	resumable bool
	name      string
	validator string
}

// probe asks for the first byte so a single request reveals size, range
// support and filename (HEAD is unreliable on many servers).
func probe(ctx context.Context, client *http.Client, rawURL string) (*probeResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	io.CopyN(io.Discard, resp.Body, 1)

	r := &probeResult{url: rawURL, size: -1}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		total, ok := parseContentRangeTotal(resp.Header.Get("Content-Range"))
		if ok {
			r.size, r.resumable = total, true
		}
	case http.StatusOK:
		r.size = resp.ContentLength
	case http.StatusRequestedRangeNotSatisfiable:
		// Empty file.
		r.size = 0
	default:
		return nil, fmt.Errorf("server returned %s", resp.Status)
	}
	r.validator = resp.Header.Get("ETag")
	if r.validator == "" {
		r.validator = resp.Header.Get("Last-Modified")
	}
	r.name = fileNameFrom(resp)
	return r, nil
}

func parseContentRangeTotal(v string) (int64, bool) {
	// bytes 0-0/12345
	i := strings.LastIndexByte(v, '/')
	if i < 0 || v[i+1:] == "*" {
		return 0, false
	}
	n, err := strconv.ParseInt(v[i+1:], 10, 64)
	return n, err == nil
}

func fileNameFrom(resp *http.Response) string {
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if n := sanitizeName(params["filename"]); n != "" {
				return n
			}
		}
	}
	return nameFromURL(resp.Request.URL)
}

func nameFromURL(u *url.URL) string {
	base := path.Base(u.Path)
	if unescaped, err := url.PathUnescape(base); err == nil {
		base = unescaped
	}
	if n := sanitizeName(base); n != "" {
		return n
	}
	return "download"
}

func sanitizeName(n string) string {
	n = strings.TrimSpace(n)
	n = strings.Map(func(r rune) rune {
		switch {
		case r < 32, strings.ContainsRune(`/\:*?"<>|`, r):
			return '_'
		}
		return r
	}, n)
	n = strings.Trim(n, ". ")
	if n == "" || n == "_" {
		return ""
	}
	if len(n) > 200 {
		n = strings.ToValidUTF8(n[:200], "")
	}
	return n
}
