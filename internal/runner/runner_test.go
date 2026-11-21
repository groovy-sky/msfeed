package runner

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/groovy-sky/msfeed/internal/config"
)

func TestRunIsIdempotentAndPreservesChangedRevision(t *testing.T) {
	var mu sync.Mutex
	etag := `"v1"`
	description := "Original"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/rss+xml")
		if req.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = fmt.Fprintf(w, `<?xml version="1.0"?><rss version="2.0"><channel><title>Test</title>`+
			`<item><guid>stable-id</guid><title>Hello</title><description>%s</description>`+
			`<link>/post/1</link></item></channel></rss>`, description)
	}))
	defer server.Close()

	dataDir := t.TempDir()
	cfg := &config.Config{
		SchemaVersion: 1, DataDir: dataDir, MaxRunSeconds: 30,
		HTTP: config.HTTP{
			TimeoutSeconds: 5, MaxBodyBytes: 1 << 20, MaxRedirects: 3,
			UserAgent: "rsscollector-test", AllowPrivateNetworks: true,
		},
		Defaults: config.Defaults{PollIntervalSeconds: 3600, MaxEntriesPerRun: 100},
		Feeds: []config.Feed{{
			ID: "test", URL: server.URL, Enabled: true,
			PollIntervalSeconds: 3600, MaxEntriesPerRun: 100,
		}},
	}
	collector := New(cfg)
	first, err := collector.Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if first.Failed != 0 || first.Feeds[0].Counts.NewEntries != 1 {
		t.Fatalf("first report = %+v", first)
	}
	entryDirs, err := os.ReadDir(filepath.Join(dataDir, "feeds", "test", "entries"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entryDirs) != 1 {
		t.Fatalf("entry directories = %d, want 1", len(entryDirs))
	}
	entryDir := filepath.Join(dataDir, "feeds", "test", "entries", entryDirs[0].Name())

	second, err := collector.Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if second.Failed != 0 || !second.Feeds[0].NotModified {
		t.Fatalf("second report = %+v", second)
	}
	assertRevisionCount(t, entryDir, 1)

	mu.Lock()
	etag = `"v2"`
	description = "Updated"
	mu.Unlock()
	third, err := collector.Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if third.Failed != 0 || third.Feeds[0].Counts.Revisions != 1 {
		t.Fatalf("third report = %+v", third)
	}
	assertRevisionCount(t, entryDir, 2)
}

func TestRunRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("this response is too large"))
	}))
	defer server.Close()
	cfg := &config.Config{
		SchemaVersion: 1, DataDir: t.TempDir(), MaxRunSeconds: 30,
		HTTP: config.HTTP{
			TimeoutSeconds: 5, MaxBodyBytes: 8, MaxRedirects: 1,
			UserAgent: "test", AllowPrivateNetworks: true,
		},
		Defaults: config.Defaults{PollIntervalSeconds: 1, MaxEntriesPerRun: 10},
		Feeds:    []config.Feed{{ID: "large", URL: server.URL, Enabled: true, PollIntervalSeconds: 1, MaxEntriesPerRun: 10}},
	}
	report, err := New(cfg).Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed != 1 {
		t.Fatalf("failed = %d, want 1", report.Failed)
	}
}

func assertRevisionCount(t *testing.T, entryDir string, want int) {
	t.Helper()
	revisions, err := os.ReadDir(filepath.Join(entryDir, "revisions"))
	if err != nil {
		t.Fatal(err)
	}
	if len(revisions) != want {
		t.Fatalf("revisions = %d, want %d", len(revisions), want)
	}
}
