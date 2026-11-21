package store

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func hashOf(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
