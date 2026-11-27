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
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/groovy-sky/msfeed/internal/classify"
	"github.com/groovy-sky/msfeed/internal/parse"
)

const dirMode = 0o700
const CompactEntryVersion = 4

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
	SchemaVersion   int      `json:"schema_version"`
	EntryKey        string   `json:"entry_key"`
	IdentityBasis   string   `json:"identity_basis"`
	Identity        string   `json:"identity,omitempty"`
	SourceID        string   `json:"source_id,omitempty"`
	URL             string   `json:"url,omitempty"`
	HasFeedContent  bool     `json:"has_feed_content,omitempty"`
	CanonicalWeek   string   `json:"canonical_week"`
	WeekAliases     []string `json:"week_aliases,omitempty"`
	CurrentRevision string   `json:"current_revision"`
	State           string   `json:"state,omitempty"`
	StateRevision   string   `json:"state_revision,omitempty"`
	Area            string   `json:"area,omitempty"`
	AreaRevision    string   `json:"area_revision,omitempty"`
	FirstSeen       string   `json:"first_seen"`
	LastSeen        string   `json:"last_seen"`
}

type EntryLocation struct {
	SchemaVersion int    `json:"schema_version"`
	EntryKey      string `json:"entry_key"`
	IdentityBasis string `json:"identity_basis"`
	Identity      string `json:"identity,omitempty"`
	CanonicalWeek string `json:"canonical_week"`
}

type EnrichmentIndex struct {
	SchemaVersion   int    `json:"schema_version"`
	EntryKey        string `json:"entry_key"`
	CurrentArtifact string `json:"current_artifact"`
	FirstEnrichedAt string `json:"first_enriched_at"`
	UpdatedAt       string `json:"updated_at"`
}

type EnrichmentRecord struct {
	SchemaVersion int    `json:"schema_version"`
	EntryKey      string `json:"entry_key"`
	EntryRevision string `json:"entry_revision"`
	Provider      string `json:"provider"`
	SourceID      string `json:"source_id"`
	SourceURL     string `json:"source_url"`
	ArticleURL    string `json:"article_url"`
	ContentType   string `json:"content_type"`
	Content       string `json:"content"`
	RetrievedAt   string `json:"retrieved_at"`
}

type ClassificationRecord struct {
	SchemaVersion int    `json:"schema_version"`
	EntryKey      string `json:"entry_key"`
	EntryRevision string `json:"entry_revision"`
	Provider      string `json:"provider"`
	State         string `json:"state"`
	Area          string `json:"area,omitempty"`
	InputSHA256   string `json:"input_sha256"`
	ClassifiedAt  string `json:"classified_at"`
}

type FeedCounts struct {
	EntriesSeen             int `json:"entries_seen"`
	NewEntries              int `json:"new_entries"`
	Revisions               int `json:"revisions"`
	Unchanged               int `json:"unchanged"`
	Enrichments             int `json:"enrichments,omitempty"`
	EnrichmentRevisions     int `json:"enrichment_revisions,omitempty"`
	EnrichmentDeferred      int `json:"enrichment_deferred,omitempty"`
	Classifications         int `json:"classifications,omitempty"`
	ClassificationRevisions int `json:"classification_revisions,omitempty"`
}

type FeedConfig struct {
	SchemaVersion int             `json:"schema_version"`
	ID            string          `json:"id"`
	URL           string          `json:"url"`
	Enrichment    *FeedEnrichment `json:"enrichment,omitempty"`
}

type FeedEnrichment struct {
	Provider           string `json:"provider"`
	URL                string `json:"url"`
	IDField            string `json:"id_field,omitempty"`
	ContentField       string `json:"content_field,omitempty"`
	ContentType        string `json:"content_type,omitempty"`
	MaxItemsPerRequest int    `json:"max_items_per_request,omitempty"`
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

func (s *Store) LatestSnapshot(
	feedID, lastSuccess, etag, lastModified string,
) (SnapshotMetadata, bool, error) {
	root := filepath.Join(s.feedDir(feedID), "snapshot-metadata")
	files, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return SnapshotMetadata{}, false, nil
	}
	if err != nil {
		return SnapshotMetadata{}, false, err
	}
	var latest SnapshotMetadata
	var latestAt time.Time
	var cutoff time.Time
	if lastSuccess != "" {
		var err error
		cutoff, err = time.Parse(time.RFC3339, lastSuccess)
		if err != nil {
			return SnapshotMetadata{}, false, fmt.Errorf(
				"invalid last success time %q: %w", lastSuccess, err,
			)
		}
	}
	found := false
	for _, file := range files {
		if file.Type().IsRegular() && strings.HasPrefix(file.Name(), ".tmp-") {
			continue
		}
		if !file.Type().IsRegular() || filepath.Ext(file.Name()) != ".json" {
			return SnapshotMetadata{}, false, fmt.Errorf(
				"snapshot metadata path %q is not a regular JSON file",
				filepath.Join(root, file.Name()),
			)
		}
		var metadata SnapshotMetadata
		if err := readJSON(filepath.Join(root, file.Name()), &metadata); err != nil {
			return SnapshotMetadata{}, false, err
		}
		retrievedAt, err := time.Parse(time.RFC3339Nano, metadata.RetrievedAt)
		if err != nil {
			return SnapshotMetadata{}, false, fmt.Errorf(
				"invalid retrieval time in snapshot metadata %s: %w",
				file.Name(), err,
			)
		}
		if metadata.FeedID != feedID || metadata.Hash == "" ||
			file.Name() != metadata.Hash+".json" {
			return SnapshotMetadata{}, false, fmt.Errorf(
				"snapshot metadata %s does not match its feed and immutable path",
				file.Name(),
			)
		}
		if !cutoff.IsZero() && retrievedAt.After(cutoff) {
			continue
		}
		if etag != "" && metadata.ETag != etag {
			continue
		}
		if lastModified != "" && metadata.LastModified != lastModified {
			continue
		}
		if !found || retrievedAt.After(latestAt) ||
			(retrievedAt.Equal(latestAt) && metadata.Hash > latest.Hash) {
			latest = metadata
			latestAt = retrievedAt
			found = true
		}
	}
	return latest, found, nil
}

func (s *Store) SaveEntry(entry parse.Entry, observedAt time.Time) (newEntry, changed bool, err error) {
	location, found, err := s.entryLocation(entry.FeedID, entry.EntryKey)
	if err != nil {
		return false, false, err
	}
	incomingWeek := entryWeek(entry, observedAt)
	identity := entryIdentity(entry)
	if found && !sameIdentity(location, entry, identity) {
		return false, false, fmt.Errorf(
			"entry key %s collides with a different identity", entry.EntryKey,
		)
	}
	week := incomingWeek
	if found {
		week = location.CanonicalWeek
	}
	base := filepath.Join(s.feedDir(entry.FeedID), "weeks", week, entry.EntryKey)
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
	if !isNew {
		if index.EntryKey != entry.EntryKey || index.IdentityBasis != entry.IdentityBasis {
			return false, false, errors.New("entry index does not match its immutable identity")
		}
		if index.Identity != "" && identity != "" && index.Identity != identity {
			return false, false, errors.New("entry index collides with a different identity")
		}
	}
	changed = index.CurrentRevision != entry.ContentHash
	now := observedAt.UTC().Format(time.RFC3339)
	if isNew {
		index = EntryIndex{
			SchemaVersion: 1, EntryKey: entry.EntryKey, IdentityBasis: entry.IdentityBasis,
			Identity: identity, CanonicalWeek: week, CurrentRevision: entry.ContentHash,
			FirstSeen: now,
		}
		newEntry = true
	} else {
		index.CurrentRevision = entry.ContentHash
		if changed {
			index.State = ""
			index.StateRevision = ""
			index.Area = ""
			index.AreaRevision = ""
		}
		if index.Identity == "" {
			index.Identity = identity
		}
		if entry.Record().PublishedAt != 0 && incomingWeek != week &&
			!containsString(index.WeekAliases, incomingWeek) {
			index.WeekAliases = append(index.WeekAliases, incomingWeek)
			sort.Strings(index.WeekAliases)
		}
	}
	index.SourceID = strings.TrimSpace(entry.SourceID)
	index.URL = entry.URL
	index.HasFeedContent = strings.TrimSpace(entry.Content) != ""
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
		if existing.SourceRecord() != record {
			return false, false, errors.New("revision file does not match its compact record")
		}
	}
	if err := writeJSON(indexPath, index); err != nil {
		return false, false, err
	}
	if !found || (location.Identity == "" && identity != "") {
		location = EntryLocation{
			SchemaVersion: 1, EntryKey: entry.EntryKey, IdentityBasis: entry.IdentityBasis,
			Identity: identity, CanonicalWeek: week,
		}
		if err := writeJSON(s.locationPath(entry.FeedID, entry.EntryKey), location); err != nil {
			return false, false, fmt.Errorf("write entry location: %w", err)
		}
	}
	return newEntry, changed, nil
}

func (s *Store) EntriesNeedingEnrichment(
	feedID, provider, sourceURL string,
) ([]parse.Entry, error) {
	current, err := s.loadCurrentEntries(feedID)
	if err != nil {
		return nil, err
	}
	entries := make([]parse.Entry, 0)
	for _, stored := range current {
		index := stored.index
		if index.SourceID == "" {
			continue
		}
		if index.HasFeedContent || stored.entry.Content != "" {
			continue
		}

		var enrichmentIndex EnrichmentIndex
		err = readJSON(filepath.Join(stored.base, "enrichment.json"), &enrichmentIndex)
		if errors.Is(err, os.ErrNotExist) {
			entries = append(entries, stored.entry)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read enrichment index for %s: %w", index.EntryKey, err)
		}
		var enrichment EnrichmentRecord
		artifactPath := filepath.Join(
			stored.base, "enrichments", enrichmentIndex.CurrentArtifact+".json",
		)
		if err := readJSON(artifactPath, &enrichment); err != nil {
			return nil, fmt.Errorf("read enrichment artifact for %s: %w", index.EntryKey, err)
		}
		if enrichment.EntryRevision == index.CurrentRevision &&
			enrichment.Provider == provider && enrichment.SourceURL == sourceURL {
			continue
		}
		entries = append(entries, stored.entry)
	}
	return entries, nil
}

func (s *Store) EntriesNeedingClassification(
	feedID, provider string,
) ([]parse.Entry, error) {
	if strings.TrimSpace(provider) == "" {
		return nil, errors.New("classification provider is required")
	}
	current, err := s.loadCurrentEntries(feedID)
	if err != nil {
		return nil, err
	}
	entries := make([]parse.Entry, 0)
	for _, stored := range current {
		entry, err := s.entryWithEnrichment(stored)
		if err != nil {
			return nil, err
		}
		inputHash := classify.InputHash(classify.Input(entry))
		var record ClassificationRecord
		err = readJSON(filepath.Join(stored.base, "classification.json"), &record)
		if errors.Is(err, os.ErrNotExist) {
			entries = append(entries, entry)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read classification for %s: %w", entry.EntryKey, err)
		}
		if record.SchemaVersion != 1 {
			return nil, fmt.Errorf(
				"unsupported classification schema version %d for %s",
				record.SchemaVersion, entry.EntryKey,
			)
		}
		if record.EntryKey != entry.EntryKey {
			return nil, fmt.Errorf("classification does not match entry %s", entry.EntryKey)
		}
		if !classify.ValidState(record.State) {
			return nil, fmt.Errorf(
				"classification for %s has invalid state %q", entry.EntryKey, record.State,
			)
		}
		if record.Area != "" && !classify.ValidArea(record.Area) {
			return nil, fmt.Errorf(
				"classification for %s has invalid area %q", entry.EntryKey, record.Area,
			)
		}
		if _, err := time.Parse(time.RFC3339, record.ClassifiedAt); err != nil {
			return nil, fmt.Errorf(
				"invalid classification time for %s: %w", entry.EntryKey, err,
			)
		}
		index := stored.index
		currentMatch := record.EntryRevision == index.CurrentRevision &&
			record.Provider == provider && record.InputSHA256 == inputHash
		indexMatch := index.StateRevision == index.CurrentRevision &&
			index.State == record.State
		areaMatch := index.AreaRevision == index.CurrentRevision &&
			index.Area == record.Area
		if record.Area != "" && currentMatch && indexMatch && areaMatch {
			continue
		}
		if index.StateRevision == index.CurrentRevision &&
			index.State != record.State && currentMatch {
			return nil, fmt.Errorf("entry index and classification disagree for %s", entry.EntryKey)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

type currentStoredEntry struct {
	entry    parse.Entry
	index    EntryIndex
	base     string
	revision parse.Record
}

func (s *Store) loadCurrentEntries(feedID string) ([]currentStoredEntry, error) {
	if err := s.validateWeeklyLocations(feedID); err != nil {
		return nil, err
	}
	root := filepath.Join(s.feedDir(feedID), "locations")
	directories, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entries := make([]currentStoredEntry, 0, len(directories))
	for _, directory := range directories {
		if !directory.Type().IsRegular() || filepath.Ext(directory.Name()) != ".json" {
			return nil, fmt.Errorf("entry location path %q is not a regular JSON file", directory.Name())
		}
		var location EntryLocation
		if err := readJSON(filepath.Join(root, directory.Name()), &location); err != nil {
			return nil, fmt.Errorf("read entry location %s: %w", directory.Name(), err)
		}
		entryKey := strings.TrimSuffix(directory.Name(), ".json")
		if location.EntryKey != entryKey {
			return nil, fmt.Errorf("entry location %s does not match its path", directory.Name())
		}
		base := filepath.Join(s.feedDir(feedID), "weeks", location.CanonicalWeek, entryKey)
		var index EntryIndex
		if err := readJSON(filepath.Join(base, "index.json"), &index); err != nil {
			return nil, fmt.Errorf("read entry index %s: %w", entryKey, err)
		}
		if index.EntryKey != entryKey || index.CanonicalWeek != location.CanonicalWeek {
			return nil, fmt.Errorf("entry index %s does not match its location", entryKey)
		}
		record, err := s.readRevision(filepath.Join(
			base, "revisions", index.CurrentRevision+".json",
		))
		if err != nil {
			return nil, fmt.Errorf("read current revision for %s: %w", index.EntryKey, err)
		}
		entries = append(entries, currentStoredEntry{
			entry: parse.Entry{
				FeedID: feedID, EntryKey: index.EntryKey, ContentHash: index.CurrentRevision,
				IdentityBasis: index.IdentityBasis, Identity: index.Identity,
				SourceID: index.SourceID, Title: record.Title, URL: record.URL,
				Summary: record.Summary, Content: record.SourceRecord().Content,
			},
			index:    index,
			base:     base,
			revision: record,
		})
	}
	return entries, nil
}

func (s *Store) entryWithEnrichment(stored currentStoredEntry) (parse.Entry, error) {
	record, err := readCurrentEnrichment(stored)
	if err != nil {
		return parse.Entry{}, err
	}
	entry := stored.entry
	if record.Content != "" && record.Content != entry.Summary {
		if entry.Content == "" {
			entry.Content = record.Content
		} else {
			entry.Content += "\n" + record.Content
		}
	}
	return entry, nil
}

func readCurrentEnrichment(stored currentStoredEntry) (EnrichmentRecord, error) {
	var index EnrichmentIndex
	err := readJSON(filepath.Join(stored.base, "enrichment.json"), &index)
	if errors.Is(err, os.ErrNotExist) {
		return EnrichmentRecord{}, nil
	}
	if err != nil {
		return EnrichmentRecord{}, fmt.Errorf(
			"read enrichment index for %s: %w", stored.entry.EntryKey, err,
		)
	}
	var record EnrichmentRecord
	if err := readJSON(
		filepath.Join(stored.base, "enrichments", index.CurrentArtifact+".json"),
		&record,
	); err != nil {
		return EnrichmentRecord{}, fmt.Errorf(
			"read enrichment artifact for %s: %w", stored.entry.EntryKey, err,
		)
	}
	if record.EntryRevision != stored.entry.ContentHash {
		return EnrichmentRecord{}, nil
	}
	return record, nil
}

func (s *Store) PublishEnrichedContent(ctx context.Context, feedID string) error {
	current, err := s.loadCurrentEntries(feedID)
	if err != nil {
		return err
	}
	for _, stored := range current {
		if err := ctx.Err(); err != nil {
			return err
		}
		if stored.index.HasFeedContent || stored.entry.Content != "" {
			continue
		}
		enrichment, err := readCurrentEnrichment(stored)
		if err != nil {
			return err
		}
		if enrichment.Content == "" {
			continue
		}
		revision := stored.revision
		if revision.Hash() != stored.index.CurrentRevision {
			return fmt.Errorf("current revision content does not match entry %s", stored.index.EntryKey)
		}
		content := enrichment.Content
		if content == revision.Summary {
			content = ""
		}
		if revision.Content == content {
			continue
		}
		revision.Content = content
		revision.ContentFromEnrichment = content != ""
		path := filepath.Join(stored.base, "revisions", stored.index.CurrentRevision+".json")
		if err := writeJSON(path, revision); err != nil {
			return fmt.Errorf("publish enrichment for %s: %w", stored.index.EntryKey, err)
		}
	}
	return nil
}

func (s *Store) validateWeeklyLocations(feedID string) error {
	weeksRoot := filepath.Join(s.feedDir(feedID), "weeks")
	weeks, err := os.ReadDir(weeksRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, week := range weeks {
		if !week.IsDir() || !validWeek(week.Name()) {
			return fmt.Errorf("week path %q is not a valid ISO week directory", week.Name())
		}
		entries, err := os.ReadDir(filepath.Join(weeksRoot, week.Name()))
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				return fmt.Errorf("weekly entry path %q is not a directory", entry.Name())
			}
			location, found, err := s.entryLocation(feedID, entry.Name())
			if err != nil {
				return err
			}
			if !found || location.CanonicalWeek != week.Name() {
				return fmt.Errorf(
					"weekly entry %s in %s is missing a matching location",
					entry.Name(), week.Name(),
				)
			}
		}
	}
	return nil
}

func (s *Store) SaveEnrichment(
	entry parse.Entry,
	record EnrichmentRecord,
) (newArtifact, changed bool, err error) {
	if entry.EntryKey == "" || entry.ContentHash == "" {
		return false, false, errors.New("entry identity and revision are required for enrichment")
	}
	if record.SchemaVersion == 0 {
		record.SchemaVersion = 1
	}
	if record.SchemaVersion != 1 {
		return false, false, fmt.Errorf("unsupported enrichment schema version %d", record.SchemaVersion)
	}
	if record.EntryKey != entry.EntryKey || record.EntryRevision != entry.ContentHash {
		return false, false, errors.New("enrichment record does not match its entry revision")
	}
	if record.Provider == "" || record.SourceID == "" || record.SourceURL == "" ||
		record.ContentType == "" || record.Content == "" || record.RetrievedAt == "" {
		return false, false, errors.New("enrichment record is incomplete")
	}
	if _, err := time.Parse(time.RFC3339Nano, record.RetrievedAt); err != nil {
		return false, false, fmt.Errorf("invalid enrichment retrieval time: %w", err)
	}

	location, found, err := s.entryLocation(entry.FeedID, entry.EntryKey)
	if err != nil {
		return false, false, err
	}
	if !found {
		return false, false, errors.New("entry location is missing")
	}
	if !sameIdentity(location, entry, entryIdentity(entry)) {
		return false, false, errors.New("entry location collides with a different identity")
	}
	base := filepath.Join(s.feedDir(entry.FeedID), "weeks", location.CanonicalWeek, entry.EntryKey)
	artifacts := filepath.Join(base, "enrichments")
	if err := os.MkdirAll(artifacts, dirMode); err != nil {
		return false, false, err
	}
	artifactHash := record.Hash()
	artifactPath := filepath.Join(artifacts, artifactHash+".json")
	_, statErr := os.Stat(artifactPath)
	switch {
	case errors.Is(statErr, os.ErrNotExist):
		if err := writeJSON(artifactPath, record); err != nil {
			return false, false, err
		}
		newArtifact = true
	case statErr != nil:
		return false, false, statErr
	default:
		var existing EnrichmentRecord
		if err := readJSON(artifactPath, &existing); err != nil {
			return false, false, fmt.Errorf("read enrichment artifact: %w", err)
		}
		if !sameEnrichment(existing, record) {
			return false, false, errors.New("enrichment artifact does not match its immutable path")
		}
	}

	indexPath := filepath.Join(base, "enrichment.json")
	var index EnrichmentIndex
	err = readJSON(indexPath, &index)
	switch {
	case errors.Is(err, os.ErrNotExist):
		index = EnrichmentIndex{
			SchemaVersion: 1, EntryKey: entry.EntryKey, CurrentArtifact: artifactHash,
			FirstEnrichedAt: record.RetrievedAt, UpdatedAt: record.RetrievedAt,
		}
		changed = true
	case err != nil:
		return false, false, fmt.Errorf("read enrichment index: %w", err)
	case index.EntryKey != entry.EntryKey:
		return false, false, errors.New("enrichment index does not match its entry")
	case index.CurrentArtifact != artifactHash:
		index.CurrentArtifact = artifactHash
		index.UpdatedAt = record.RetrievedAt
		changed = true
	}
	if changed {
		if err := writeJSON(indexPath, index); err != nil {
			return newArtifact, false, err
		}
	}
	return newArtifact, changed, nil
}

func (record EnrichmentRecord) Hash() string {
	record.RetrievedAt = ""
	data, err := json.Marshal(record)
	if err != nil {
		panic("JSON enrichment content must always marshal: " + err.Error())
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func sameEnrichment(left, right EnrichmentRecord) bool {
	left.RetrievedAt = ""
	right.RetrievedAt = ""
	return left == right
}

func (s *Store) SaveClassification(
	entry parse.Entry,
	record ClassificationRecord,
) (newRecord, changed bool, err error) {
	if entry.EntryKey == "" || entry.ContentHash == "" {
		return false, false, errors.New("entry identity and revision are required for classification")
	}
	if record.SchemaVersion == 0 {
		record.SchemaVersion = 1
	}
	if record.SchemaVersion != 1 {
		return false, false, fmt.Errorf("unsupported classification schema version %d", record.SchemaVersion)
	}
	if record.EntryKey != entry.EntryKey || record.EntryRevision != entry.ContentHash {
		return false, false, errors.New("classification record does not match its entry revision")
	}
	if strings.TrimSpace(record.Provider) == "" {
		return false, false, errors.New("classification provider is required")
	}
	if !classify.ValidState(record.State) {
		return false, false, fmt.Errorf("invalid classification state %q", record.State)
	}
	if !classify.ValidArea(record.Area) {
		return false, false, fmt.Errorf("invalid classification area %q", record.Area)
	}
	inputHash := classify.InputHash(classify.Input(entry))
	if record.InputSHA256 != inputHash {
		return false, false, errors.New("classification input hash does not match the entry")
	}
	if _, err := time.Parse(time.RFC3339Nano, record.ClassifiedAt); err != nil {
		return false, false, fmt.Errorf("invalid classification time: %w", err)
	}

	location, found, err := s.entryLocation(entry.FeedID, entry.EntryKey)
	if err != nil {
		return false, false, err
	}
	if !found {
		return false, false, errors.New("entry location is missing")
	}
	if !sameIdentity(location, entry, entryIdentity(entry)) {
		return false, false, errors.New("entry location collides with a different identity")
	}
	base := filepath.Join(s.feedDir(entry.FeedID), "weeks", location.CanonicalWeek, entry.EntryKey)
	indexPath := filepath.Join(base, "index.json")
	var index EntryIndex
	if err := readJSON(indexPath, &index); err != nil {
		return false, false, fmt.Errorf("read entry index: %w", err)
	}
	if index.EntryKey != entry.EntryKey || index.CurrentRevision != entry.ContentHash {
		return false, false, errors.New("entry index does not match the classified revision")
	}

	recordPath := filepath.Join(base, "classification.json")
	var existing ClassificationRecord
	err = readJSON(recordPath, &existing)
	switch {
	case errors.Is(err, os.ErrNotExist):
		newRecord = true
		changed = true
	case err != nil:
		return false, false, fmt.Errorf("read classification: %w", err)
	case !sameClassification(existing, record):
		changed = true
	}
	if changed {
		if err := writeJSON(recordPath, record); err != nil {
			return newRecord, false, err
		}
	}

	indexChanged := index.State != record.State ||
		index.StateRevision != record.EntryRevision ||
		index.Area != record.Area ||
		index.AreaRevision != record.EntryRevision
	if indexChanged {
		index.State = record.State
		index.StateRevision = record.EntryRevision
		index.Area = record.Area
		index.AreaRevision = record.EntryRevision
		if err := writeJSON(indexPath, index); err != nil {
			return newRecord, false, err
		}
		changed = true
	}
	return newRecord, changed, nil
}

func sameClassification(left, right ClassificationRecord) bool {
	left.ClassifiedAt = ""
	right.ClassifiedAt = ""
	return left == right
}

func (s *Store) UpgradeEntryStorage(ctx context.Context, feedID string, allowIncomplete bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	complete := true

	legacyRoot := filepath.Join(s.feedDir(feedID), "entries")
	legacyEntries, err := os.ReadDir(legacyRoot)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	for _, directory := range legacyEntries {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !directory.IsDir() {
			return false, fmt.Errorf("entry path %q is not a directory", directory.Name())
		}
		legacyPath := filepath.Join(legacyRoot, directory.Name())
		directoryComplete, err := s.upgradeEntryDirectory(
			ctx, legacyPath, directory.Name(), true,
		)
		if err != nil {
			return false, err
		}
		if !directoryComplete {
			recovered, recoverErr := s.removeRecoveredLegacyEntry(
				feedID, directory.Name(), legacyPath,
			)
			if recoverErr != nil {
				return false, recoverErr
			}
			if recovered {
				continue
			}
			if !allowIncomplete {
				return false, fmt.Errorf("entry index %s is missing", directory.Name())
			}
			complete = false
			continue
		}
		if err := s.moveEntryToWeek(feedID, directory.Name(), legacyPath); err != nil {
			return false, err
		}
	}
	if len(legacyEntries) > 0 {
		remaining, err := os.ReadDir(legacyRoot)
		if err != nil {
			return false, err
		}
		if len(remaining) == 0 {
			if err := os.Remove(legacyRoot); err != nil {
				return false, fmt.Errorf("remove empty legacy entry root: %w", err)
			}
		}
	}

	weeksRoot := filepath.Join(s.feedDir(feedID), "weeks")
	weeks, err := os.ReadDir(weeksRoot)
	if errors.Is(err, os.ErrNotExist) {
		return complete, nil
	}
	if err != nil {
		return false, err
	}
	for _, week := range weeks {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !week.IsDir() || !validWeek(week.Name()) {
			return false, fmt.Errorf("week path %q is not a valid ISO week directory", week.Name())
		}
		weekPath := filepath.Join(weeksRoot, week.Name())
		entries, err := os.ReadDir(weekPath)
		if err != nil {
			return false, err
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if !entry.IsDir() {
				return false, fmt.Errorf("entry path %q is not a directory", entry.Name())
			}
			directory := filepath.Join(weekPath, entry.Name())
			directoryComplete, err := s.upgradeEntryDirectory(
				ctx, directory, entry.Name(), allowIncomplete,
			)
			if err != nil {
				return false, err
			}
			if !directoryComplete {
				complete = false
				continue
			}
			if err := s.ensureLocation(feedID, week.Name(), entry.Name()); err != nil {
				return false, err
			}
		}
	}
	return complete, ctx.Err()
}

func (s *Store) removeRecoveredLegacyEntry(feedID, entryKey, legacyPath string) (bool, error) {
	location, found, err := s.entryLocation(feedID, entryKey)
	if err != nil || !found {
		return false, err
	}
	destination := filepath.Join(
		s.feedDir(feedID), "weeks", location.CanonicalWeek, entryKey,
	)
	files, err := os.ReadDir(legacyPath)
	if err != nil {
		return false, err
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), ".tmp-") && file.Type().IsRegular() {
			continue
		}
		switch file.Name() {
		case "revisions":
			if !file.IsDir() {
				return false, errors.New("legacy recovered revisions path is not a directory")
			}
			revisions, err := os.ReadDir(filepath.Join(legacyPath, "revisions"))
			if err != nil {
				return false, err
			}
			for _, revision := range revisions {
				if strings.HasPrefix(revision.Name(), ".tmp-") && revision.Type().IsRegular() {
					continue
				}
				if !revision.Type().IsRegular() {
					return false, errors.New("legacy recovered entry contains a non-regular revision")
				}
				record, err := s.readRevision(filepath.Join(legacyPath, "revisions", revision.Name()))
				if err != nil {
					return false, err
				}
				existing, err := readCompactRevision(filepath.Join(
					destination, "revisions", record.Hash()+".json",
				))
				if err != nil {
					return false, fmt.Errorf("recovered entry revision is not present: %w", err)
				}
				if existing.SourceRecord() != record.SourceRecord() {
					return false, errors.New("recovered entry revision differs from weekly entry")
				}
			}
		default:
			return false, fmt.Errorf("legacy recovered entry contains unexpected path %s", file.Name())
		}
	}
	if err := os.RemoveAll(legacyPath); err != nil {
		return false, fmt.Errorf("remove recovered legacy entry %s: %w", entryKey, err)
	}
	return true, nil
}

func (s *Store) moveEntryToWeek(feedID, entryKey, source string) error {
	var index EntryIndex
	if err := readJSON(filepath.Join(source, "index.json"), &index); err != nil {
		return fmt.Errorf("read migrated entry index %s: %w", entryKey, err)
	}
	week, err := indexWeek(feedID, source, index)
	if err != nil {
		return err
	}
	index.CanonicalWeek = week
	index.WeekAliases = nil
	index.Identity = indexIdentity(index)
	if err := writeJSON(filepath.Join(source, "index.json"), index); err != nil {
		return fmt.Errorf("update migrated entry index %s: %w", entryKey, err)
	}
	destination := filepath.Join(s.feedDir(feedID), "weeks", week, entryKey)
	if err := os.MkdirAll(filepath.Dir(destination), dirMode); err != nil {
		return err
	}
	if _, err := os.Stat(destination); err == nil {
		return fmt.Errorf("weekly entry path already exists for %s in %s", entryKey, week)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(source, destination); err != nil {
		return fmt.Errorf("move entry %s to week %s: %w", entryKey, week, err)
	}
	if err := s.ensureLocation(feedID, week, entryKey); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureLocation(feedID, week, entryKey string) error {
	base := filepath.Join(s.feedDir(feedID), "weeks", week, entryKey)
	var index EntryIndex
	if err := readJSON(filepath.Join(base, "index.json"), &index); err != nil {
		return fmt.Errorf("read weekly entry index %s: %w", entryKey, err)
	}
	if index.EntryKey != entryKey || (index.CanonicalWeek != "" && index.CanonicalWeek != week) {
		return fmt.Errorf("weekly entry index %s does not match %s", entryKey, week)
	}
	if index.CanonicalWeek == "" {
		index.CanonicalWeek = week
		if err := writeJSON(filepath.Join(base, "index.json"), index); err != nil {
			return fmt.Errorf("set canonical week for %s: %w", entryKey, err)
		}
	}
	path := s.locationPath(feedID, entryKey)
	var existing EntryLocation
	err := readJSON(path, &existing)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return writeJSON(path, EntryLocation{
			SchemaVersion: 1, EntryKey: entryKey, IdentityBasis: index.IdentityBasis,
			Identity: index.Identity, CanonicalWeek: week,
		})
	case err != nil:
		return fmt.Errorf("read entry location %s: %w", entryKey, err)
	case existing.EntryKey != entryKey || existing.CanonicalWeek != week ||
		existing.IdentityBasis != index.IdentityBasis:
		return fmt.Errorf("entry location %s conflicts with its weekly index", entryKey)
	case existing.Identity != "" && index.Identity != "" && existing.Identity != index.Identity:
		return fmt.Errorf("entry location %s collides with a different identity", entryKey)
	default:
		return nil
	}
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
		if stored.SourceRecord() != record.SourceRecord() {
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

func (s *Store) locationPath(feedID, entryKey string) string {
	return filepath.Join(s.feedDir(feedID), "locations", entryKey+".json")
}

func (s *Store) entryLocation(feedID, entryKey string) (EntryLocation, bool, error) {
	var location EntryLocation
	path := s.locationPath(feedID, entryKey)
	err := readJSON(path, &location)
	if errors.Is(err, os.ErrNotExist) {
		return EntryLocation{}, false, nil
	}
	if err != nil {
		return EntryLocation{}, false, fmt.Errorf("read entry location %s: %w", entryKey, err)
	}
	if location.SchemaVersion != 1 || location.EntryKey != entryKey ||
		!validWeek(location.CanonicalWeek) {
		return EntryLocation{}, false, fmt.Errorf("invalid entry location %s", entryKey)
	}
	return location, true, nil
}

func entryWeek(entry parse.Entry, fallback time.Time) string {
	publishedAt := entry.Record().PublishedAt
	if publishedAt == 0 {
		return isoWeek(fallback)
	}
	return isoWeek(time.Unix(publishedAt, 0).UTC())
}

func indexWeek(feedID, directory string, index EntryIndex) (string, error) {
	record, err := readCompactRevision(filepath.Join(
		directory, "revisions", index.CurrentRevision+".json",
	))
	if err != nil {
		return "", fmt.Errorf("read migrated entry revision for %s: %w", index.EntryKey, err)
	}
	if record.PublishedAt != 0 {
		return isoWeek(time.Unix(record.PublishedAt, 0).UTC()), nil
	}
	firstSeen, err := time.Parse(time.RFC3339, index.FirstSeen)
	if err != nil {
		return "", fmt.Errorf(
			"entry %s in feed %s has neither a publication date nor valid first_seen: %w",
			index.EntryKey, feedID, err,
		)
	}
	return isoWeek(firstSeen), nil
}

func isoWeek(value time.Time) string {
	year, week := value.UTC().ISOWeek()
	return fmt.Sprintf("%04d-W%02d", year, week)
}

func validWeek(value string) bool {
	if len(value) != 8 || value[4:6] != "-W" {
		return false
	}
	year, err := strconv.Atoi(value[:4])
	if err != nil || year < 1 {
		return false
	}
	week, err := strconv.Atoi(value[6:])
	return err == nil && week >= 1 && week <= 53
}

func entryIdentity(entry parse.Entry) string {
	if entry.Identity != "" {
		return entry.Identity
	}
	switch entry.IdentityBasis {
	case "id":
		return strings.TrimSpace(entry.SourceID)
	case "link":
		return entry.URL
	default:
		return ""
	}
}

func indexIdentity(index EntryIndex) string {
	switch index.IdentityBasis {
	case "id":
		return strings.TrimSpace(index.SourceID)
	case "link":
		return index.URL
	default:
		return ""
	}
}

func sameIdentity(location EntryLocation, entry parse.Entry, identity string) bool {
	if location.IdentityBasis != entry.IdentityBasis {
		return false
	}
	return location.Identity == "" || identity == "" || location.Identity == identity
}

func containsString(values []string, value string) bool {
	for _, existing := range values {
		if existing == value {
			return true
		}
	}
	return false
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
