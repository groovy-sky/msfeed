package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestSaveEntryStoresOnlyCompactRecord(t *testing.T) {
	st := New(t.TempDir())
	entry := parse.Entry{
		SchemaVersion: 1,
		EntryKey:      "entry-key",
		FeedID:        "test",
		IdentityBasis: "id",
		Title:         "A compact entry",
		URL:           "https://example.com/update",
		PublishedAt:   "2026-11-12T16:00:02Z",
		Summary:       "<p>Important <strong>summary</strong>.</p>",
		Content:       "<p>Long body that is already retained in the raw snapshot.</p>",
		Provenance: parse.Provenance{
			SourceURL: "https://example.com/feed",
		},
	}
	entry.ContentHash = entry.Record().Hash()
	if _, _, err := st.SaveEntry(entry, time.Now()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(
		st.Root(), "feeds", "test", "entries", entry.EntryKey, "revisions",
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
	if len(decoded) != 4 {
		t.Fatalf("compact revision has %d fields: %s", len(decoded), data)
	}
	for _, field := range []string{"url", "title", "published_at", "summary"} {
		if _, ok := decoded[field]; !ok {
			t.Fatalf("compact revision is missing %q: %s", field, data)
		}
	}
	if decoded["published_at"] != float64(1794499202) ||
		decoded["summary"] != "Important summary." {
		t.Fatalf("compact revision = %s", data)
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
		Content:       "<p>Bulky legacy body.</p>",
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

	if err := st.UpgradeEntryStorage("test"); err != nil {
		t.Fatal(err)
	}
	record := legacy.Record()
	compactPath := filepath.Join(directory, "revisions", record.Hash()+".json")
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("legacy revision still exists: %v", err)
	}
	if _, err := os.Stat(compactPath); err != nil {
		t.Fatalf("compact revision missing: %v", err)
	}
	var index EntryIndex
	if err := readJSON(filepath.Join(directory, "index.json"), &index); err != nil {
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
}

func hashOf(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
