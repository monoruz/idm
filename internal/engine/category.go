package engine

import (
	"path/filepath"
	"strings"

	"idm/internal/store"
)

func DefaultCategories() []store.Category {
	return []store.Category{
		{Name: "Music", Extensions: []string{"mp3", "flac", "wav", "aac", "ogg", "oga", "m4a", "wma", "opus", "alac", "aiff"}},
		{Name: "Video", Extensions: []string{"mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "m4v", "mpg", "mpeg", "ts", "3gp"}},
		{Name: "Compressed", Extensions: []string{"zip", "rar", "7z", "tar", "gz", "tgz", "bz2", "xz", "zst", "lz", "lzma", "cab"}},
		{Name: "Documents", Extensions: []string{"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods", "txt", "rtf", "epub", "csv", "md"}},
		{Name: "Programs", Extensions: []string{"exe", "msi", "dmg", "pkg", "deb", "rpm", "apk", "appimage", "iso", "img", "bin", "run", "sh"}},
		{Name: "Images", Extensions: []string{"jpg", "jpeg", "png", "gif", "webp", "svg", "bmp", "tif", "tiff", "heic", "avif", "raw"}},
	}
}

const OtherCategory = "Other"

func categorize(cats []store.Category, name string) string {
	lower := strings.ToLower(name)
	ext := strings.TrimPrefix(filepath.Ext(lower), ".")
	// Multi-part extensions such as .tar.gz still resolve through "gz".
	for _, c := range cats {
		for _, e := range c.Extensions {
			if e == ext {
				return c.Name
			}
		}
	}
	return OtherCategory
}
