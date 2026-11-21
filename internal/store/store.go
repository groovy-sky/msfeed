package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/groovy-sky/msfeed/internal/parse"
)

const dirMode = 0o700

type Store struct {
	root string
}

type State struct {
	SchemaVersion       int    `json:"schema_version,omitempty"`
	ETag                string `json:"etag,omitempty"`
	LastModified        string `json:"last_modified,omitempty"`
	EffectiveURL        string `json:"effective_url,omitempty"`
	LastSuccess         string `json:"last_success,omitempty"`
	LastAttempt         string `json:"last_attempt,omitempty"`
	NextRetryAt         string `json:"next_retry_at,omitempty"`
	ConsecutiveErrors   int    `json:"consecutive_errors,omitempty"`
	LastError           string `json:"last_error,omitempty"`
	FailureClass        string `json:"failure_class,omitempty"`
	RetryExhausted      bool   `json:"retry_exhausted,omitempty"`
	PendingSnapshot     string `json:"pending_snapshot,omitempty"`
	PendingETag         string `json:"pending_etag,omitempty"`
	PendingLastMod      string `json:"pending_last_modified,omitempty"`
	PendingEffectiveURL string `json:"pending_effective_url,omitempty"`
	PendingRetrievedAt  string `json:"pending_retrieved_at,omitempty"`
	PendingEncoding     string `json:"pending_content_encoding,omitempty"`
	PendingContentType  string `json:"pending_content_type,omitempty"`
	PendingOffset       int    `json:"pending_offset,omitempty"`
}

type SnapshotMetadata struct {
	SchemaVersion   int      `json:"schema_version"`
	FeedID          string   `json:"feed_id"`
	Hash            string   `json:"hash"`
	Bytes           int64    `json:"bytes"`
	RequestedURL    string   `json:"requested_url"`
	EffectiveURL    string   `json:"effective_url"`
	Status          int      `json:"status"`
	ContentType     string   `json:"content_type,omitempty"`
	ContentEncoding string   `json:"content_encoding,omitempty"`
	ETag            string   `json:"etag,omitempty"`
	LastModified    string   `json:"last_modified,omitempty"`
	RetrievedAt     string   `json:"retrieved_at"`
	Redirects       []string `json:"redirects,omitempty"`
}

type EntryIndex struct {
	SchemaVersion   int    `json:"schema_version"`
	EntryKey        string `json:"entry_key"`
	IdentityBasis   string `json:"identity_basis"`
	CurrentRevision string `json:"current_revision"`
	FirstSeen       string `json:"first_seen"`
	LastSeen        string `json:"last_seen"`
}

type FeedCounts struct {
	EntriesSeen int `json:"entries_seen"`
	NewEntries  int `json:"new_entries"`
	Revisions   int `json:"revisions"`
	Unchanged   int `json:"unchanged"`
}

type FeedConfig struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id"`
	URL           string `json:"url"`
}

func New(root string) *Store {
	return &Store{root: filepath.Clean(root)}
}

func (s *Store) Root() string {
	return s.root
}

func (s *Store) Initialize() error {
	for _, path := range []string{s.root, filepath.Join(s.root, "feeds"), filepath.Join(s.root, "runs")} {
		if err := os.MkdirAll(path, dirMode); err != nil {
			return fmt.Errorf("create %s: %w", path, err)
		}
	}
	return nil
}

func (s *Store) SaveFeed(feed FeedConfig) error {
	dir := s.feedDir(feed.ID)
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return err
	}
	return writeJSON(filepath.Join(dir, "feed.json"), feed)
}

func (s *Store) LoadState(feedID string) (State, error) {
	var state State
	err := readJSON(filepath.Join(s.feedDir(feedID), "state.json"), &state)
	if errors.Is(err, os.ErrNotExist) {
		return State{}, nil
	}
	return state, err
}

func (s *Store) SaveState(feedID string, state State) error {
	dir := s.feedDir(feedID)
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return err
	}
	return writeJSON(filepath.Join(dir, "state.json"), state)
}

func (s *Store) SaveSnapshot(feedID string, body []byte, metadata SnapshotMetadata) error {
	dir := s.feedDir(feedID)
	for _, sub := range []string{"snapshots", "snapshot-metadata"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), dirMode); err != nil {
			return err
		}
	}
	sum := sha256.Sum256(body)
	metadata.Hash = hex.EncodeToString(sum[:])
	metadata.Bytes = int64(len(body))
	snapshotPath := filepath.Join(dir, "snapshots", metadata.Hash+".xml")
	if _, err := os.Stat(snapshotPath); errors.Is(err, os.ErrNotExist) {
		if err := writeFile(snapshotPath, body); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if _, err := s.ReadSnapshot(feedID, metadata.Hash); err != nil {
		return err
	}
	metadataPath := filepath.Join(dir, "snapshot-metadata", metadata.Hash+".json")
	if _, err := os.Stat(metadataPath); errors.Is(err, os.ErrNotExist) {
		return writeJSON(metadataPath, metadata)
	} else if err != nil {
		return err
	}
	return nil
}

func (s *Store) ReadSnapshot(feedID, hash string) ([]byte, error) {
	var expected [sha256.Size]byte
	if len(hash) != hex.EncodedLen(len(expected)) {
		return nil, errors.New("invalid snapshot hash length")
	}
	if _, err := hex.Decode(expected[:], []byte(hash)); err != nil {
		return nil, fmt.Errorf("invalid snapshot hash: %w", err)
	}
	path := filepath.Join(s.feedDir(feedID), "snapshots", hash+".xml")
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if actual := sha256.Sum256(body); actual != expected {
		return nil, fmt.Errorf("corrupt immutable file %s: expected %s, found %x", path, hash, actual)
	}
	return body, nil
}

func (s *Store) SaveEntry(entry parse.Entry, observedAt time.Time) (newEntry, changed bool, err error) {
	base := filepath.Join(s.feedDir(entry.FeedID), "entries", entry.EntryKey)
	revisions := filepath.Join(base, "revisions")
	if err := os.MkdirAll(revisions, dirMode); err != nil {
		return false, false, err
	}
	indexPath := filepath.Join(base, "index.json")
	var index EntryIndex
	err = readJSON(indexPath, &index)
	isNew := errors.Is(err, os.ErrNotExist)
	if err != nil && !isNew {
		return false, false, fmt.Errorf("read entry index: %w", err)
	}
	changed = index.CurrentRevision != entry.ContentHash
	now := observedAt.UTC().Format(time.RFC3339)
	if isNew {
		index = EntryIndex{
			SchemaVersion: 1, EntryKey: entry.EntryKey, IdentityBasis: entry.IdentityBasis,
			CurrentRevision: entry.ContentHash, FirstSeen: now,
		}
		newEntry = true
	} else {
		index.CurrentRevision = entry.ContentHash
	}
	index.LastSeen = now
	revisionPath := filepath.Join(revisions, entry.ContentHash+".json")
	if _, statErr := os.Stat(revisionPath); errors.Is(statErr, os.ErrNotExist) {
		if err := writeJSON(revisionPath, entry); err != nil {
			return false, false, err
		}
	} else if statErr != nil {
		return false, false, statErr
	} else {
		var existing parse.Entry
		if err := readJSON(revisionPath, &existing); err != nil {
			return false, false, fmt.Errorf("read existing revision: %w", err)
		}
		if existing.ContentHash != entry.ContentHash || existing.EntryKey != entry.EntryKey {
			return false, false, errors.New("revision file does not match its immutable path")
		}
	}
	if err := writeJSON(indexPath, index); err != nil {
		return false, false, err
	}
	return newEntry, changed, nil
}

func (s *Store) feedDir(feedID string) string {
	return filepath.Join(s.root, "feeds", feedID)
}

func readJSON(path string, target any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeFile(path, data)
}

func writeFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	cleanup := true
	defer func() {
		_ = tmp.Close()
		if cleanup {
			_ = os.Remove(name)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	if dirFile, err := os.Open(dir); err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}
	cleanup = false
	return nil
}

func (s *Store) SaveRun(id string, report any) error {
	return writeJSON(filepath.Join(s.root, "runs", id+".json"), report)
}

func (s *Store) CheckCapacity(bytesNeeded int64) (low bool, err error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(s.root, &stat); err != nil {
		return false, fmt.Errorf("check available disk space: %w", err)
	}
	available := int64(stat.Bavail) * int64(stat.Bsize)
	reserve := bytesNeeded*2 + 1<<20
	if available < reserve {
		return true, fmt.Errorf("insufficient disk space: need %d bytes, have %d", reserve, available)
	}
	return available < 64<<20, nil
}
