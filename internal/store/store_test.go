package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groovy-sky/msfeed/internal/classify"
	"github.com/groovy-sky/msfeed/internal/parse"
)

func TestSaveSnapshotRejectsCorruptExistingArtifact(t *testing.T) {
	st := New(t.TempDir())
	if err := st.Initialize(); err != nil {
		t.Fatal(err)
	}
	body := []byte("<rss/>")
	metadata := SnapshotMetadata{SchemaVersion: 1, FeedID: "test", RetrievedAt: time.Now().UTC().Format(time.RFC3339)}
	if err := st.SaveSnapshot("test", body, metadata); err != nil {
		t.Fatal(err)
	}
	sum := hashOf(body)
	path := filepath.Join(st.Root(), "feeds", "test", "snapshots", sum+".xml")
	if err := os.WriteFile(path, []byte("<corrupt/>"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveSnapshot("test", body, metadata); err == nil {
		t.Fatal("corrupt immutable snapshot was accepted")
	}
}

func TestLatestSnapshotUsesLastSuccessfulFetch(t *testing.T) {
	st := New(t.TempDir())
	good := []byte("<rss>good</rss>")
	bad := []byte("<rss>incomplete")
	if err := st.SaveSnapshot("test", good, SnapshotMetadata{
		SchemaVersion: 1, FeedID: "test", ETag: `"good"`,
		RetrievedAt: "2026-11-22T10:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveSnapshot("test", bad, SnapshotMetadata{
		SchemaVersion: 1, FeedID: "test", ETag: `"rejected"`,
		RetrievedAt: "2026-11-22T11:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	metadata, found, err := st.LatestSnapshot(
		"test", "2026-11-22T10:30:00Z", `"good"`, "",
	)
	if err != nil || !found {
		t.Fatalf("latest snapshot: found=%t error=%v", found, err)
	}
	if metadata.Hash != hashOf(good) {
		t.Fatalf("selected metadata hash = %s, want %s", metadata.Hash, hashOf(good))
	}
}

func TestSaveEntryStoresOnlyCompactRecord(t *testing.T) {
	st := New(t.TempDir())
	body := strings.Repeat("Article text. ", 30) + "Final sentence."
	entry := parse.Entry{
		SchemaVersion: 1,
		EntryKey:      "entry-key",
		FeedID:        "test",
		IdentityBasis: "id",
		Title:         "A compact entry",
		URL:           "https://example.com/update",
		PublishedAt:   "2026-11-12T16:00:02Z",
		Summary:       "<p>Important <strong>summary</strong>.</p>",
		Content:       "<p>" + body + "</p>",
		Provenance: parse.Provenance{
			SourceURL: "https://example.com/feed",
		},
	}
	entry.ContentHash = entry.Record().Hash()
	if _, _, err := st.SaveEntry(entry, time.Now()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(
		entryDirectory(t, st, "test", entry.EntryKey), "revisions",
		entry.ContentHash+".json",
	)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 5 {
		t.Fatalf("compact revision has %d fields: %s", len(decoded), data)
	}
	for _, field := range []string{"url", "title", "published_at", "summary", "content"} {
		if _, ok := decoded[field]; !ok {
			t.Fatalf("compact revision is missing %q: %s", field, data)
		}
	}
	if decoded["published_at"] != float64(1794499202) ||
		decoded["summary"] != "Important summary." ||
		decoded["content"] != body {
		t.Fatalf("compact revision = %s", data)
	}
}

func TestSaveEnrichmentStoresImmutableProviderContent(t *testing.T) {
	st := New(t.TempDir())
	entry := parse.Entry{
		EntryKey: "entry-key", FeedID: "test", IdentityBasis: "id",
		SourceID: "42", Title: "An entry", URL: "https://example.com/update",
		Summary: "An RSS excerpt.",
	}
	entry.ContentHash = entry.Record().Hash()
	if _, _, err := st.SaveEntry(entry, time.Now()); err != nil {
		t.Fatal(err)
	}
	record := EnrichmentRecord{
		SchemaVersion: 1, EntryKey: entry.EntryKey, EntryRevision: entry.ContentHash,
		Provider: "odata", SourceID: entry.SourceID,
		SourceURL: "https://example.com/api", ArticleURL: entry.URL,
		ContentType: "text/plain", Content: "The complete provider content.",
		RetrievedAt: "2026-11-22T10:00:00Z",
	}
	newArtifact, changed, err := st.SaveEnrichment(entry, record)
	if err != nil || !newArtifact || !changed {
		t.Fatalf("first save: new=%t changed=%t error=%v", newArtifact, changed, err)
	}
	record.RetrievedAt = "2026-11-22T11:00:00Z"
	newArtifact, changed, err = st.SaveEnrichment(entry, record)
	if err != nil || newArtifact || changed {
		t.Fatalf("idempotent save: new=%t changed=%t error=%v", newArtifact, changed, err)
	}

	base := entryDirectory(t, st, "test", entry.EntryKey)
	artifacts, err := os.ReadDir(filepath.Join(base, "enrichments"))
	if err != nil || len(artifacts) != 1 {
		t.Fatalf("artifacts = %d, error = %v", len(artifacts), err)
	}
	var stored EnrichmentRecord
	if err := readJSON(filepath.Join(base, "enrichments", artifacts[0].Name()), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Content != "The complete provider content." ||
		stored.RetrievedAt != "2026-11-22T10:00:00Z" {
		t.Fatalf("stored enrichment = %+v", stored)
	}

	record.Content = "A later complete provider version."
	newArtifact, changed, err = st.SaveEnrichment(entry, record)
	if err != nil || !newArtifact || !changed {
		t.Fatalf("changed save: new=%t changed=%t error=%v", newArtifact, changed, err)
	}
	artifacts, err = os.ReadDir(filepath.Join(base, "enrichments"))
	if err != nil || len(artifacts) != 2 {
		t.Fatalf("changed artifacts = %d, error = %v", len(artifacts), err)
	}
}

func TestEntriesNeedingEnrichmentUsesCurrentRevisionAndProvider(t *testing.T) {
	st := New(t.TempDir())
	entry := parse.Entry{
		EntryKey: "entry-key", FeedID: "test", IdentityBasis: "id",
		SourceID: "42", Title: "An entry", URL: "https://example.com/update",
		Summary: "Source excerpt.",
	}
	entry.ContentHash = entry.Record().Hash()
	if _, _, err := st.SaveEntry(entry, time.Now()); err != nil {
		t.Fatal(err)
	}
	pending, err := st.EntriesNeedingEnrichment("test", "odata", "https://example.com/api")
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending before enrichment = %d, error = %v", len(pending), err)
	}
	record := EnrichmentRecord{
		SchemaVersion: 1, EntryKey: entry.EntryKey, EntryRevision: entry.ContentHash,
		Provider: "odata", SourceID: entry.SourceID, SourceURL: "https://example.com/api",
		ArticleURL: entry.URL, ContentType: "text/plain", Content: "Complete text.",
		RetrievedAt: "2026-11-22T10:00:00Z",
	}
	if _, _, err := st.SaveEnrichment(entry, record); err != nil {
		t.Fatal(err)
	}
	pending, err = st.EntriesNeedingEnrichment("test", "odata", "https://example.com/api")
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after enrichment = %d, error = %v", len(pending), err)
	}

	pending, err = st.EntriesNeedingEnrichment("test", "other", "https://example.com/api")
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending for different provider = %d, error = %v", len(pending), err)
	}
	entry.Summary = "Changed source excerpt."
	entry.ContentHash = entry.Record().Hash()
	if _, _, err := st.SaveEntry(entry, time.Now()); err != nil {
		t.Fatal(err)
	}
	pending, err = st.EntriesNeedingEnrichment("test", "odata", "https://example.com/api")
	if err != nil || len(pending) != 1 || pending[0].ContentHash != entry.ContentHash {
		t.Fatalf("pending after revision = %+v, error = %v", pending, err)
	}

	feedContentEntry := entry
	feedContentEntry.EntryKey = "feed-content"
	feedContentEntry.SourceID = "43"
	feedContentEntry.Content = entry.Summary
	feedContentEntry.ContentHash = feedContentEntry.Record().Hash()
	if _, _, err := st.SaveEntry(feedContentEntry, time.Now()); err != nil {
		t.Fatal(err)
	}
	pending, err = st.EntriesNeedingEnrichment("test", "odata", "https://example.com/api")
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range pending {
		if candidate.EntryKey == feedContentEntry.EntryKey {
			t.Fatal("entry with feed-supplied content was queued for provider enrichment")
		}
	}
}

func TestClassificationFollowsCurrentEntryRevision(t *testing.T) {
	st := New(t.TempDir())
	entry := parse.Entry{
		EntryKey: "entry-key", FeedID: "test", IdentityBasis: "id",
		Title: "Public Preview: Example", Summary: "A private preview update.",
	}
	entry.ContentHash = entry.Record().Hash()
	if _, _, err := st.SaveEntry(entry, time.Now()); err != nil {
		t.Fatal(err)
	}

	pending, err := st.EntriesNeedingClassification("test", classify.ProviderLaya)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending before classification = %d, error = %v", len(pending), err)
	}
	record := ClassificationRecord{
		EntryKey: entry.EntryKey, EntryRevision: entry.ContentHash,
		Provider: classify.ProviderLaya, State: classify.StatePreview,
		Area:         classify.AreaAIAndAgents,
		InputSHA256:  classify.InputHash(classify.Input(entry)),
		ClassifiedAt: "2026-11-27T10:00:00Z",
	}
	if newRecord, changed, err := st.SaveClassification(entry, record); err != nil ||
		!newRecord || !changed {
		t.Fatalf("first save: new=%t changed=%t error=%v", newRecord, changed, err)
	}
	pending, err = st.EntriesNeedingClassification("test", classify.ProviderLaya)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after classification = %d, error = %v", len(pending), err)
	}

	base := entryDirectory(t, st, "test", entry.EntryKey)
	var index EntryIndex
	if err := readJSON(filepath.Join(base, "index.json"), &index); err != nil {
		t.Fatal(err)
	}
	if index.State != classify.StatePreview || index.StateRevision != entry.ContentHash ||
		index.Area != classify.AreaAIAndAgents || index.AreaRevision != entry.ContentHash {
		t.Fatalf("entry state = %+v", index)
	}
	var stored ClassificationRecord
	if err := readJSON(filepath.Join(base, "classification.json"), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.State != classify.StatePreview || stored.Area != classify.AreaAIAndAgents ||
		stored.Provider != classify.ProviderLaya {
		t.Fatalf("classification = %+v", stored)
	}

	stored.Area = ""
	if err := writeJSON(filepath.Join(base, "classification.json"), stored); err != nil {
		t.Fatal(err)
	}
	index.Area = ""
	index.AreaRevision = ""
	if err := writeJSON(filepath.Join(base, "index.json"), index); err != nil {
		t.Fatal(err)
	}
	pending, err = st.EntriesNeedingClassification("test", classify.ProviderLaya)
	if err != nil || len(pending) != 1 {
		t.Fatalf("state-only classification was not queued for area backfill = %d, error = %v", len(pending), err)
	}
	record.Area = classify.AreaAIAndAgents
	if newRecord, changed, err := st.SaveClassification(entry, record); err != nil ||
		newRecord || !changed {
		t.Fatalf("area backfill: new=%t changed=%t error=%v", newRecord, changed, err)
	}

	entry.Title = "Generally Available: Example"
	entry.ContentHash = entry.Record().Hash()
	if _, changed, err := st.SaveEntry(entry, time.Now()); err != nil || !changed {
		t.Fatalf("save revised entry: changed=%t error=%v", changed, err)
	}
	index = EntryIndex{}
	if err := readJSON(filepath.Join(base, "index.json"), &index); err != nil {
		t.Fatal(err)
	}
	if index.State != "" || index.StateRevision != "" ||
		index.Area != "" || index.AreaRevision != "" {
		t.Fatalf("revised entry retained stale classification: %+v", index)
	}
	pending, err = st.EntriesNeedingClassification("test", classify.ProviderLaya)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending after source revision = %d, error = %v", len(pending), err)
	}
}

func TestUpgradeEntryStorageCompactsLegacyRevisions(t *testing.T) {
	st := New(t.TempDir())
	legacy := parse.Entry{
		SchemaVersion: 1,
		EntryKey:      "legacy-key",
		FeedID:        "test",
		IdentityBasis: "id",
		SourceID:      "legacy-id",
		Title:         "Legacy entry",
		URL:           "https://example.com/legacy",
		PublishedAt:   "2026-11-12T16:00:02Z",
		Summary:       "<p>Legacy <em>summary</em>.</p>",
		Content:       "<p>Complete legacy body.</p>",
	}
	legacyHash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	legacy.ContentHash = legacyHash
	directory := filepath.Join(st.Root(), "feeds", "test", "entries", legacy.EntryKey)
	if err := os.MkdirAll(filepath.Join(directory, "revisions"), dirMode); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(directory, "index.json"), EntryIndex{
		SchemaVersion:   1,
		EntryKey:        legacy.EntryKey,
		IdentityBasis:   legacy.IdentityBasis,
		CurrentRevision: legacyHash,
	}); err != nil {
		t.Fatal(err)
	}
	legacyPath := filepath.Join(directory, "revisions", legacyHash+".json")
	if err := writeJSON(legacyPath, legacy); err != nil {
		t.Fatal(err)
	}

	complete, err := st.UpgradeEntryStorage(context.Background(), "test", false)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatal("entry storage upgrade is incomplete")
	}
	record := legacy.Record()
	destination := filepath.Join(
		st.Root(), "feeds", "test", "weeks", "2026-W46", legacy.EntryKey,
	)
	compactPath := filepath.Join(destination, "revisions", record.Hash()+".json")
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("legacy revision still exists: %v", err)
	}
	if _, err := os.Stat(compactPath); err != nil {
		t.Fatalf("compact revision missing: %v", err)
	}
	var index EntryIndex
	if err := readJSON(filepath.Join(destination, "index.json"), &index); err != nil {
		t.Fatal(err)
	}
	if index.CurrentRevision != record.Hash() {
		t.Fatalf("current revision = %s, want %s", index.CurrentRevision, record.Hash())
	}
	stored, err := readCompactRevision(compactPath)
	if err != nil {
		t.Fatal(err)
	}
	if stored != record {
		t.Fatalf("stored record = %#v, want %#v", stored, record)
	}
	if stored.Summary != "Legacy summary." || stored.Content != "Complete legacy body." {
		t.Fatalf("legacy summary or article text was lost: %#v", stored)
	}
}

func TestUpgradeEntryStorageDefersIncompleteEntries(t *testing.T) {
	st := New(t.TempDir())
	entry := parse.Entry{
		SchemaVersion: 1,
		EntryKey:      "interrupted-key",
		FeedID:        "test",
		IdentityBasis: "id",
		Title:         "Interrupted entry",
		URL:           "https://example.com/interrupted",
		PublishedAt:   "2026-11-12T16:00:02Z",
		Summary:       "Interrupted summary",
	}
	entry.ContentHash = entry.Record().Hash()
	directory := filepath.Join(st.Root(), "feeds", "test", "entries", entry.EntryKey)
	revisions := filepath.Join(directory, "revisions")
	if err := os.MkdirAll(revisions, dirMode); err != nil {
		t.Fatal(err)
	}
	legacyPath := filepath.Join(revisions, entry.ContentHash+".json")
	if err := writeJSON(legacyPath, entry.Record()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, ".tmp-abandoned"), []byte("partial"), dirMode); err != nil {
		t.Fatal(err)
	}
	complete, err := st.UpgradeEntryStorage(context.Background(), "test", true)
	if err != nil {
		t.Fatal(err)
	}
	if complete {
		t.Fatal("entry storage upgrade completed without an index")
	}
	if _, err := os.Stat(filepath.Join(directory, ".tmp-abandoned")); !os.IsNotExist(err) {
		t.Fatalf("abandoned index temp remains: %v", err)
	}

	if _, _, err := st.SaveEntry(entry, time.Now()); err != nil {
		t.Fatal(err)
	}
	complete, err = st.UpgradeEntryStorage(context.Background(), "test", false)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatal("entry storage upgrade did not recover")
	}
}

func TestUpgradeEntryStorageIgnoresTemporaryRevisions(t *testing.T) {
	st := New(t.TempDir())
	entry := parse.Entry{
		EntryKey: "temp-key", FeedID: "test", IdentityBasis: "id",
		Title: "Temporary cleanup", URL: "https://example.com/temp", Summary: "Summary",
	}
	entry.ContentHash = entry.Record().Hash()
	if _, _, err := st.SaveEntry(entry, time.Now()); err != nil {
		t.Fatal(err)
	}
	revisions := filepath.Join(
		entryDirectory(t, st, "test", entry.EntryKey), "revisions",
	)
	tempPath := filepath.Join(revisions, ".tmp-abandoned")
	if err := os.WriteFile(tempPath, []byte("partial"), dirMode); err != nil {
		t.Fatal(err)
	}
	complete, err := st.UpgradeEntryStorage(context.Background(), "test", false)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatal("entry storage upgrade is incomplete")
	}
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Fatalf("abandoned revision temp remains: %v", err)
	}
}

func TestUpgradeEntryStorageHonorsCancellation(t *testing.T) {
	st := New(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	complete, err := st.UpgradeEntryStorage(ctx, "test", false)
	if complete {
		t.Fatal("canceled upgrade reported complete")
	}
	if err != context.Canceled {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestSaveEntryKeepsCanonicalWeekAndRecordsCorrectedWeek(t *testing.T) {
	st := New(t.TempDir())
	entry := parse.Entry{
		EntryKey: "stable-key", FeedID: "test", IdentityBasis: "id",
		Identity: "stable-id", SourceID: "stable-id", Title: "Dated entry",
		PublishedAt: "2026-11-12T16:00:02Z", Summary: "Original.",
	}
	entry.ContentHash = entry.Record().Hash()
	observed := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	if _, _, err := st.SaveEntry(entry, observed); err != nil {
		t.Fatal(err)
	}
	original := entryDirectory(t, st, "test", entry.EntryKey)
	if !strings.HasSuffix(filepath.ToSlash(original), "/weeks/2026-W46/"+entry.EntryKey) {
		t.Fatalf("entry directory = %s", original)
	}

	entry.PublishedAt = "2026-11-23T16:00:02Z"
	entry.Summary = "Corrected."
	entry.ContentHash = entry.Record().Hash()
	if newEntry, changed, err := st.SaveEntry(entry, observed.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	} else if newEntry || !changed {
		t.Fatalf("corrected save: new=%t changed=%t", newEntry, changed)
	}
	if current := entryDirectory(t, st, "test", entry.EntryKey); current != original {
		t.Fatalf("entry moved from %s to %s", original, current)
	}
	var index EntryIndex
	if err := readJSON(filepath.Join(original, "index.json"), &index); err != nil {
		t.Fatal(err)
	}
	if index.CanonicalWeek != "2026-W46" ||
		len(index.WeekAliases) != 1 || index.WeekAliases[0] != "2026-W48" {
		t.Fatalf("week index = %+v", index)
	}
}

func TestSaveEntryRejectsIdentityCollision(t *testing.T) {
	st := New(t.TempDir())
	entry := parse.Entry{
		EntryKey: "collision-key", FeedID: "test", IdentityBasis: "id",
		Identity: "first-id", SourceID: "first-id", Summary: "First.",
		PublishedAt: "2026-11-12T16:00:02Z",
	}
	entry.ContentHash = entry.Record().Hash()
	if _, _, err := st.SaveEntry(entry, time.Now()); err != nil {
		t.Fatal(err)
	}

	collision := entry
	collision.Identity = "second-id"
	collision.SourceID = "second-id"
	if _, _, err := st.SaveEntry(collision, time.Now()); err == nil {
		t.Fatal("different identity reused an existing entry key")
	}
}

func entryDirectory(t *testing.T, st *Store, feedID, entryKey string) string {
	t.Helper()
	location, found, err := st.entryLocation(feedID, entryKey)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("entry location %s is missing", entryKey)
	}
	return filepath.Join(
		st.Root(), "feeds", feedID, "weeks", location.CanonicalWeek, entryKey,
	)
}

func hashOf(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
