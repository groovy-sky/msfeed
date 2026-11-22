package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/groovy-sky/msfeed/internal/parse"
)

const dirMode = 0o700
const CompactEntryVersion = 2

type Store struct {
	root string
}

type State struct {
	SchemaVersion       int    `json:"schema_version,omitempty"`
	EntryStorageVersion int    `json:"entry_storage_version,omitempty"`
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
	record := entry.Record()
	if record.Hash() != entry.ContentHash {
		return false, false, errors.New("entry content hash does not match its compact record")
	}
	if _, statErr := os.Stat(revisionPath); errors.Is(statErr, os.ErrNotExist) {
		if err := writeJSON(revisionPath, record); err != nil {
			return false, false, err
		}
	} else if statErr != nil {
		return false, false, statErr
	} else {
		existing, err := s.readRevision(revisionPath)
		if err != nil {
			return false, false, fmt.Errorf("read existing revision: %w", err)
		}
		if existing != record {
			return false, false, errors.New("revision file does not match its compact record")
		}
	}
	if err := writeJSON(indexPath, index); err != nil {
		return false, false, err
	}
	return newEntry, changed, nil
}

func (s *Store) UpgradeEntryStorage(ctx context.Context, feedID string, allowIncomplete bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	entriesRoot := filepath.Join(s.feedDir(feedID), "entries")
	entries, err := os.ReadDir(entriesRoot)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	complete := true
	for _, directory := range entries {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !directory.IsDir() {
			return false, fmt.Errorf("entry path %q is not a directory", directory.Name())
		}
		directoryComplete, err := s.upgradeEntryDirectory(
			ctx, filepath.Join(entriesRoot, directory.Name()), directory.Name(), allowIncomplete,
		)
		if err != nil {
			return false, err
		}
		if !directoryComplete {
			complete = false
		}
	}
	return complete, ctx.Err()
}

func (s *Store) upgradeEntryDirectory(
	ctx context.Context,
	directory, entryKey string,
	allowIncomplete bool,
) (bool, error) {
	if err := removeTemporaryFiles(directory); err != nil {
		return false, err
	}
	indexPath := filepath.Join(directory, "index.json")
	var index EntryIndex
	if err := readJSON(indexPath, &index); err != nil {
		if allowIncomplete && errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("read entry index %s: %w", entryKey, err)
	}
	if index.EntryKey != entryKey {
		return false, fmt.Errorf("entry index %s does not match its directory", entryKey)
	}

	revisions := filepath.Join(directory, "revisions")
	files, err := os.ReadDir(revisions)
	if err != nil {
		return false, fmt.Errorf("read revisions for %s: %w", entryKey, err)
	}
	hashes := make(map[string]string, len(files))
	remove := make([]string, 0)
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if strings.HasPrefix(file.Name(), ".tmp-") && file.Type().IsRegular() {
			if err := os.Remove(filepath.Join(revisions, file.Name())); err != nil {
				return false, fmt.Errorf("remove temporary revision %s: %w", file.Name(), err)
			}
			continue
		}
		if !file.Type().IsRegular() || filepath.Ext(file.Name()) != ".json" {
			return false, fmt.Errorf("revision path %q is not a regular JSON file", filepath.Join(revisions, file.Name()))
		}
		oldHash := strings.TrimSuffix(file.Name(), ".json")
		path := filepath.Join(revisions, file.Name())
		legacy, err := isLegacyRevision(path)
		if err != nil {
			return false, err
		}
		if !legacy {
			record, err := readCompactRevision(path)
			if err != nil {
				return false, err
			}
			if record.Hash() != oldHash {
				return false, fmt.Errorf("compact revision %s does not match its immutable path", path)
			}
			hashes[oldHash] = oldHash
			continue
		}

		var entry parse.Entry
		if err := readJSON(path, &entry); err != nil {
			return false, fmt.Errorf("read legacy revision %s: %w", path, err)
		}
		if entry.EntryKey != entryKey || entry.ContentHash != oldHash {
			return false, fmt.Errorf("legacy revision %s does not match its immutable path", path)
		}
		record := entry.Record()
		newHash := record.Hash()
		hashes[oldHash] = newHash
		newPath := filepath.Join(revisions, newHash+".json")
		if err := writeCompactRevisionIfMissing(newPath, record); err != nil {
			return false, err
		}
		remove = append(remove, path)
	}

	current := hashes[index.CurrentRevision]
	if current == "" {
		return false, fmt.Errorf("entry index %s references a missing revision %s", entryKey, index.CurrentRevision)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if current != index.CurrentRevision {
		index.CurrentRevision = current
		if err := writeJSON(indexPath, index); err != nil {
			return false, fmt.Errorf("update entry index %s: %w", entryKey, err)
		}
	}
	for _, path := range remove {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if err := os.Remove(path); err != nil {
			return false, fmt.Errorf("remove legacy revision %s: %w", path, err)
		}
	}
	return true, nil
}

func removeTemporaryFiles(directory string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("read entry directory %s: %w", directory, err)
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".tmp-") {
			continue
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("temporary entry path %q is not a regular file", filepath.Join(directory, entry.Name()))
		}
		if err := os.Remove(filepath.Join(directory, entry.Name())); err != nil {
			return fmt.Errorf("remove temporary entry file %s: %w", entry.Name(), err)
		}
	}
	return nil
}

func (s *Store) readRevision(path string) (parse.Record, error) {
	legacy, err := isLegacyRevision(path)
	if err != nil {
		return parse.Record{}, err
	}
	if legacy {
		var entry parse.Entry
		if err := readJSON(path, &entry); err != nil {
			return parse.Record{}, err
		}
		return entry.Record(), nil
	}
	return readCompactRevision(path)
}

func isLegacyRevision(path string) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	var header struct {
		SchemaVersion int    `json:"schema_version"`
		ContentHash   string `json:"content_hash"`
		EntryKey      string `json:"entry_key"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return false, fmt.Errorf("decode revision format %s: %w", path, err)
	}
	return header.SchemaVersion != 0 || header.ContentHash != "" || header.EntryKey != "", nil
}

func readCompactRevision(path string) (parse.Record, error) {
	var record parse.Record
	if err := readJSON(path, &record); err != nil {
		return parse.Record{}, fmt.Errorf("read compact revision %s: %w", path, err)
	}
	return record, nil
}

func writeCompactRevisionIfMissing(path string, record parse.Record) error {
	existing, err := os.Stat(path)
	switch {
	case err == nil:
		if !existing.Mode().IsRegular() {
			return fmt.Errorf("compact revision %s is not a regular file", path)
		}
		stored, err := readCompactRevision(path)
		if err != nil {
			return err
		}
		if stored != record {
			return fmt.Errorf("compact revision %s already contains different content", path)
		}
		return nil
	case errors.Is(err, os.ErrNotExist):
		return writeJSON(path, record)
	default:
		return err
	}
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
