// Package store persists queues, downloads and settings in a bbolt database.
package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

type Status string

const (
	StatusQueued      Status = "queued"
	StatusDownloading Status = "downloading"
	StatusPaused      Status = "paused"
	StatusCompleted   Status = "completed"
	StatusError       Status = "error"
)

// Segment is a byte range [Start, End) of which [Start, Pos) is on disk.
// End is -1 when the total size is unknown.
type Segment struct {
	Start int64 `json:"s"`
	End   int64 `json:"e"`
	Pos   int64 `json:"p"`
}

type Download struct {
	ID          uint64    `json:"id"`
	URL         string    `json:"url"`
	Mirrors     []string  `json:"mirrors,omitempty"`
	QueueID     uint64    `json:"queue"`
	Position    int64     `json:"pos"`
	DirOverride string    `json:"dirOverride,omitempty"` // user-chosen folder; empty = automatic
	Dir         string    `json:"dir,omitempty"`         // resolved folder, set on first start
	FileName    string    `json:"name,omitempty"`
	Category    string    `json:"cat,omitempty"`
	Size        int64     `json:"size"` // -1 unknown
	Downloaded  int64     `json:"done"`
	Resumable   bool      `json:"resumable"`
	Connections int       `json:"conns"`
	Segments    []Segment `json:"segs,omitempty"`
	Validator   string    `json:"validator,omitempty"` // ETag or Last-Modified of the first probe
	Status      Status    `json:"status"`
	Error       string    `json:"err,omitempty"`
	CreatedAt   time.Time `json:"created"`
	CompletedAt time.Time `json:"completed,omitzero"`
}

type Queue struct {
	ID            uint64 `json:"id"`
	Name          string `json:"name"`
	SpeedLimit    int64  `json:"limit"` // bytes/s, 0 = unlimited
	MaxConcurrent int    `json:"max"`
	Running       bool   `json:"running"`
	Default       bool   `json:"default"` // preselected in forms
	Builtin       bool   `json:"builtin"` // cannot be deleted
}

type Category struct {
	Name       string   `json:"name"`
	Extensions []string `json:"ext"`
}

type Settings struct {
	BaseDir            string     `json:"baseDir"`
	Organize           bool       `json:"organize"`
	Categories         []Category `json:"categories"`
	DefaultConnections int        `json:"conns"`
	PasswordSalt       []byte     `json:"salt,omitempty"`
	PasswordHash       []byte     `json:"hash,omitempty"`
	SessionSecret      []byte     `json:"secret,omitempty"`
}

var (
	bDownloads = []byte("downloads")
	bQueues    = []byte("queues")
	bMeta      = []byte("meta")
	kSettings  = []byte("settings")
)

var ErrNotFound = errors.New("not found")

type Store struct{ db *bolt.DB }

func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if errors.Is(err, bolt.ErrTimeout) {
		return nil, fmt.Errorf("database %s is in use by another idm process (stop the server first)", path)
	}
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bDownloads, bQueues, bMeta} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func put(tx *bolt.Tx, bucket []byte, id uint64, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return tx.Bucket(bucket).Put(itob(id), data)
}

func loadAll[T any](s *Store, bucket []byte) ([]*T, error) {
	var out []*T
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucket).ForEach(func(_, v []byte) error {
			item := new(T)
			if err := json.Unmarshal(v, item); err != nil {
				return err
			}
			out = append(out, item)
			return nil
		})
	})
	return out, err
}

func (s *Store) Downloads() ([]*Download, error) { return loadAll[Download](s, bDownloads) }
func (s *Store) Queues() ([]*Queue, error)       { return loadAll[Queue](s, bQueues) }

// SaveDownloads writes all given downloads in a single transaction, assigning
// IDs to new ones (ID == 0).
func (s *Store) SaveDownloads(ds ...*Download) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bDownloads)
		for _, d := range ds {
			if d.ID == 0 {
				id, err := b.NextSequence()
				if err != nil {
					return err
				}
				d.ID = id
			}
			if err := put(tx, bDownloads, d.ID, d); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) DeleteDownload(id uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bDownloads).Delete(itob(id)) })
}

func (s *Store) SaveQueue(q *Queue) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if q.ID == 0 {
			id, err := tx.Bucket(bQueues).NextSequence()
			if err != nil {
				return err
			}
			q.ID = id
		}
		return put(tx, bQueues, q.ID, q)
	})
}

func (s *Store) DeleteQueue(id uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bQueues).Delete(itob(id)) })
}

func (s *Store) Settings() (*Settings, error) {
	var st *Settings
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bMeta).Get(kSettings)
		if v == nil {
			return ErrNotFound
		}
		st = new(Settings)
		return json.Unmarshal(v, st)
	})
	return st, err
}

func (s *Store) SaveSettings(st *Settings) error {
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bMeta).Put(kSettings, data) })
}
