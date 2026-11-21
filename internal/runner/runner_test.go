package runner

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	cfg := runnerConfig(t, server.URL)
	cfg.DataDir = dataDir
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

func TestRunProcessesPendingSnapshotAcrossEntryBatches(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("ETag", `"batch"`)
		_, _ = w.Write([]byte(`<?xml version="1.0"?><rss version="2.0"><channel><title>Batch</title>
			<item><guid>1</guid><title>one</title></item>
			<item><guid>2</guid><title>two</title></item>
			<item><guid>3</guid><title>three</title></item>
		</channel></rss>`))
	}))
	defer server.Close()
	cfg := runnerConfig(t, server.URL)
	cfg.Feeds[0].MaxEntriesPerRun = 1

	collector := New(cfg)
	first, err := collector.Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if first.Feeds[0].Status != "partial" || first.Feeds[0].Counts.NewEntries != 1 || !first.BudgetReached {
		t.Fatalf("first report = %+v", first)
	}
	second, err := collector.Run(context.Background(), "", false)
	if err != nil {
		t.Fatal(err)
	}
	third, err := collector.Run(context.Background(), "", false)
	if err != nil {
		t.Fatal(err)
	}
	if second.Requests != 0 || third.Requests != 0 || third.Feeds[0].Status != "completed" {
		t.Fatalf("reports = %+v, %+v", second, third)
	}
	if requests.Load() != 1 {
		t.Fatalf("network requests = %d, want 1", requests.Load())
	}
}

func TestRunContinuesAfterOneFeedFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/bad" {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`<?xml version="1.0"?><rss version="2.0"><channel><title>ok</title></channel></rss>`))
	}))
	defer server.Close()
	cfg := runnerConfig(t, server.URL+"/bad")
	cfg.Feeds = append(cfg.Feeds, config.Feed{
		ID: "good", URL: server.URL + "/good", Enabled: true,
		PollIntervalSeconds: 3600, MaxEntriesPerRun: 10,
	})
	report, err := New(cfg).Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed != 1 || len(report.Feeds) != 2 || report.Feeds[1].Status != "completed" {
		t.Fatalf("report = %+v", report)
	}
}

func TestRetryAfterIsPersisted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "60")
		http.Error(w, "busy", http.StatusTooManyRequests)
	}))
	defer server.Close()
	cfg := runnerConfig(t, server.URL)
	report, err := New(cfg).Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed != 1 || report.Feeds[0].FailureClass != "transport" {
		t.Fatalf("report = %+v", report)
	}
	state, err := New(cfg).Store.LoadState("test")
	if err != nil {
		t.Fatal(err)
	}
	next, err := time.Parse(time.RFC3339, state.NextRetryAt)
	if err != nil || time.Until(next) < 50*time.Second {
		t.Fatalf("next retry = %q, error = %v", state.NextRetryAt, err)
	}
}

func TestRunRejectsContention(t *testing.T) {
	cfg := runnerConfig(t, "https://example.com/feed")
	collector := New(cfg)
	if err := collector.Store.Initialize(); err != nil {
		t.Fatal(err)
	}
	lock, err := acquireLock(filepath.Join(cfg.DataDir, "collector.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer releaseLock(lock)
	_, err = collector.Run(context.Background(), "", true)
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("error = %v, want ErrLocked", err)
	}
}

func TestRunRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("this response is too large"))
	}))
	defer server.Close()
	cfg := runnerConfig(t, server.URL)
	cfg.HTTP.MaxBodyBytes = 8
	report, err := New(cfg).Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}

	if report.Failed != 1 {
		t.Fatalf("failed = %d, want 1", report.Failed)
	}
}

func TestRunPreservesMalformedSnapshotAsPending(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`<rss><channel><item>`))
	}))
	defer server.Close()
	cfg := runnerConfig(t, server.URL)
	report, err := New(cfg).Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed != 1 || report.Feeds[0].FailureClass != "parse" {
		t.Fatalf("report = %+v", report)
	}
	collector := New(cfg)
	state, err := collector.Store.LoadState("test")
	if err != nil {
		t.Fatal(err)
	}
	if state.PendingSnapshot == "" {
		t.Fatal("malformed snapshot was not retained as pending")
	}
	if _, err := collector.Store.ReadSnapshot("test", state.PendingSnapshot); err != nil {
		t.Fatalf("pending snapshot cannot be read: %v", err)
	}
}

func TestRunEnforcesFeedAndRequestBudgets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`<?xml version="1.0"?><rss version="2.0"><channel><title>ok</title></channel></rss>`))
	}))
	defer server.Close()
	newConfig := func(maxFeeds int) *config.Config {
		cfg := runnerConfig(t, server.URL+"/one")
		cfg.MaxFeedsPerRun = maxFeeds
		cfg.Feeds = append(cfg.Feeds, config.Feed{
			ID: "two", URL: server.URL + "/two", Enabled: true,
			PollIntervalSeconds: 3600, MaxEntriesPerRun: 10,
		})
		return cfg
	}
	feedLimited, err := New(newConfig(1)).Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if !feedLimited.BudgetReached || len(feedLimited.Feeds) != 1 {
		t.Fatalf("feed-limited report = %+v", feedLimited)
	}

	requestCfg := newConfig(0)
	requestCfg.HTTP.MaxRequestsPerRun = 1
	requestLimited, err := New(requestCfg).Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if requestLimited.Requests != 1 || !requestLimited.BudgetReached || requestLimited.Feeds[1].Status != "deferred" {
		t.Fatalf("request-limited report = %+v", requestLimited)
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

func runnerConfig(t *testing.T, feedURL string) *config.Config {
	t.Helper()
	return &config.Config{
		SchemaVersion: 1, DataDir: t.TempDir(), MaxRunSeconds: 30,
		HTTP: config.HTTP{
			TimeoutSeconds: 5, MaxBodyBytes: 1 << 20, MaxRedirects: 3,
			MaxRequestsPerRun: 100, UserAgent: "rsscollector-test", AllowPrivateNetworks: true,
		},
		Retry:    config.Retry{MaxAttempts: 3, InitialBackoffSeconds: 1, MaxBackoffSeconds: 60},
		Defaults: config.Defaults{PollIntervalSeconds: 3600, MaxEntriesPerRun: 100},
		Feeds: []config.Feed{{
			ID: "test", URL: feedURL, Enabled: true,
			PollIntervalSeconds: 3600, MaxEntriesPerRun: 100,
		}},
	}
}
