package web

import (
	"crypto/rand"
	"fmt"
	"html/template"
	"strings"

	"idm/internal/engine"
	"idm/internal/store"
)

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// RandomSecret is exported for first-run initialisation.
func RandomSecret() []byte { return randomBytes(32) }

var funcs = template.FuncMap{
	"bytes":  humanBytes,
	"speed":  func(n int64) string { return humanBytes(n) + "/s" },
	"eta":    humanETA,
	"kb":     func(n int64) int64 { return n >> 10 },
	"join":   strings.Join,
	"status": statusText,
	"icon":   categoryIcon,
	"pct":    func(f float64) string { return fmt.Sprintf("%.1f", f) },
	"name": func(d engine.DownloadView) string {
		if d.FileName != "" {
			return d.FileName
		}
		return d.URL
	},
	"limit": func(n int64) string {
		if n <= 0 {
			return "Unlimited"
		}
		return humanBytes(n) + "/s"
	},
}

func humanBytes(n int64) string {
	if n < 0 {
		return "?"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}

func humanETA(sec int64) string {
	switch {
	case sec < 0:
		return ""
	case sec < 60:
		return fmt.Sprintf("%ds", sec)
	case sec < 3600:
		return fmt.Sprintf("%dm %ds", sec/60, sec%60)
	default:
		return fmt.Sprintf("%dh %dm", sec/3600, sec%3600/60)
	}
}

func statusText(d engine.DownloadView) string {
	switch {
	case d.Active && d.Status == store.StatusDownloading:
		return "Downloading"
	case d.Active:
		return "Stopping"
	}
	switch d.Status {
	case store.StatusQueued:
		return "Queued"
	case store.StatusPaused:
		return "Paused"
	case store.StatusCompleted:
		return "Completed"
	case store.StatusError:
		return "Error"
	}
	return string(d.Status)
}

func categoryIcon(cat string) string {
	switch cat {
	case "Music":
		return "i-music"
	case "Video":
		return "i-video"
	case "Compressed":
		return "i-archive"
	case "Documents":
		return "i-doc"
	case "Programs":
		return "i-app"
	case "Images":
		return "i-image"
	}
	return "i-file"
}
