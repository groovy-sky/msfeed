package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groovy-sky/msfeed/internal/classify"
	"github.com/groovy-sky/msfeed/internal/config"
	"github.com/groovy-sky/msfeed/internal/parse"
	"github.com/groovy-sky/msfeed/internal/store"
)

func TestRunIsIdempotentAndPreservesChangedRevision(t *testing.T) {
	var mu sync.Mutex
	etag := `"v1"`
	prefix := strings.Repeat("The first paragraph stays the same. ", 20)
	ending := "Original ending."
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/rss+xml")
		if req.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = fmt.Fprintf(w, `<?xml version="1.0"?><rss version="2.0"`+
			` xmlns:content="http://purl.org/rss/1.0/modules/content/"><channel><title>Test</title>`+
			`<item><guid>stable-id</guid><title>Hello</title><description>Short summary.</description>`+
			`<content:encoded><![CDATA[<p>%s%s</p>]]></content:encoded>`+
			`<link>/post/1</link></item></channel></rss>`, prefix, ending)
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
	state, err := collector.Store.LoadState("test")
	if err != nil {
		t.Fatal(err)
	}
	if state.EntryStorageVersion != store.CompactEntryVersion {
		t.Fatalf("entry storage version = %d, want %d", state.EntryStorageVersion, store.CompactEntryVersion)
	}
	entryDirs, err := filepath.Glob(filepath.Join(dataDir, "feeds", "test", "weeks", "*", "*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entryDirs) != 1 {
		t.Fatalf("entry directories = %d, want 1", len(entryDirs))
	}
	entryDir := entryDirs[0]

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
	ending = "Updated ending."
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

func TestRunBackfillsStatesAfterNotModified(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Type", "application/rss+xml")
		if request.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = fmt.Fprint(w, `<?xml version="1.0"?><rss version="2.0"><channel>
			<title>Test</title>
			<item><guid>one</guid><title>Generally Available: New feature</title></item>
			<item><guid>two</guid><title>Retirement: Old feature</title></item>
			</channel></rss>`)
	}))
	defer server.Close()

	cfg := runnerConfig(t, server.URL)
	collector := New(cfg)
	first, err := collector.Run(context.Background(), "", true)
	if err != nil || first.Failed != 0 || first.Feeds[0].Counts.NewEntries != 2 {
		t.Fatalf("first report = %+v, error = %v", first, err)
	}

	classifier := &recordingClassifier{}
	collector.Classifier = classifier
	second, err := collector.Run(context.Background(), "", true)
	if err != nil || second.Failed != 0 ||
		!second.Feeds[0].NotModified ||
		second.Feeds[0].Counts.Classifications != 2 {
		t.Fatalf("second report = %+v, error = %v", second, err)
	}
	if classifier.Calls() != 2 {
		t.Fatalf("classification calls = %d, want 2", classifier.Calls())
	}

	entries, err := filepath.Glob(filepath.Join(
		cfg.DataDir, "feeds", "test", "weeks", "*", "*",
	))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("entry directories = %d", len(entries))
	}
	states := make(map[string]bool)
	for _, directory := range entries {
		var index store.EntryIndex
		if err := readJSONFile(filepath.Join(directory, "index.json"), &index); err != nil {
			t.Fatal(err)
		}
		states[index.State] = true
		if index.StateRevision != index.CurrentRevision {
			t.Fatalf("state revision = %s, current revision = %s", index.StateRevision, index.CurrentRevision)
		}
		if _, err := os.Stat(filepath.Join(directory, "classification.json")); err != nil {
			t.Fatal(err)
		}
	}
	if !states[classify.StateGenerallyAvailable] || !states[classify.StateRetirement] {
		t.Fatalf("states = %v", states)
	}

	third, err := collector.Run(context.Background(), "", true)
	if err != nil || third.Failed != 0 ||
		third.Feeds[0].Counts.Classifications != 0 {
		t.Fatalf("third report = %+v, error = %v", third, err)
	}
	if classifier.Calls() != 2 {
		t.Fatalf("idempotent run made %d classification calls", classifier.Calls())
	}
}

type recordingClassifier struct {
	mu    sync.Mutex
	calls int
}

func (*recordingClassifier) Provider() string {
	return classify.ProviderLaya
}

func (c *recordingClassifier) Classify(_ context.Context, entry parse.Entry) (classify.Result, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	state := classify.StateOther
	switch {
	case strings.Contains(entry.Title, "Generally Available"):
		state = classify.StateGenerallyAvailable
	case strings.Contains(entry.Title, "Retirement"):
		state = classify.StateRetirement
	}
	input := classify.Input(entry)
	return classify.Result{State: state, InputSHA256: classify.InputHash(input)}, nil
}

func (c *recordingClassifier) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestRunStoresODataEnrichmentSeparately(t *testing.T) {
	var mu sync.Mutex
	apiRequests := 0
	feedBody := `<?xml version="1.0"?><rss version="2.0"
		xmlns:content="http://purl.org/rss/1.0/modules/content/"><channel><title>Test</title>
		<item><guid>one</guid><title>One</title><link>/one</link><description>First excerpt.</description></item>
		<item><guid>two</guid><title>Two</title><link>/two</link><description>Second excerpt.</description></item>
		<item><guid>three</guid><title>Three</title><link>/three</link><description>Third excerpt.</description>
		<content:encoded><![CDATA[<p>Third body supplied by the feed.</p>]]></content:encoded></item>
		</channel></rss>`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/feed":
			w.Header().Set("Content-Type", "application/rss+xml")
			_, _ = fmt.Fprint(w, feedBody)
		case "/api":
			mu.Lock()
			apiRequests++
			mu.Unlock()
			if strings.Contains(request.URL.Query().Get("$filter"), "'three'") {
				t.Error("provider request included an entry that has feed-supplied content")
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"value":[
				{"id":"one","description":"<p>First complete body.</p><script>ignored()</script>"},
				{"id":"two","description":"<p>Second complete body.</p>"}
			]}`)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	cfg := runnerConfig(t, server.URL+"/feed")
	cfg.Feeds[0].Enrichment = &config.Enrichment{
		Provider: "odata", URL: server.URL + "/api",
		IDField: "id", ContentField: "description", ContentType: "text/html",
		MaxItemsPerRequest: 100,
	}
	collector := New(cfg)
	first, err := collector.Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if first.Failed != 0 || first.Requests != 2 ||
		first.Feeds[0].Counts.NewEntries != 3 ||
		first.Feeds[0].Counts.Enrichments != 2 {
		t.Fatalf("first report = %+v", first)
	}
	mu.Lock()
	if apiRequests != 1 {
		t.Fatalf("API requests = %d, want 1", apiRequests)
	}
	mu.Unlock()

	var storedFeed store.FeedConfig
	if err := readJSONFile(
		filepath.Join(cfg.DataDir, "feeds", "test", "feed.json"), &storedFeed,
	); err != nil {
		t.Fatal(err)
	}
	if storedFeed.Enrichment == nil ||
		storedFeed.Enrichment.Provider != "odata" ||
		storedFeed.Enrichment.URL != server.URL+"/api" {
		t.Fatalf("stored feed enrichment = %+v", storedFeed.Enrichment)
	}

	entries, err := filepath.Glob(filepath.Join(cfg.DataDir, "feeds", "test", "weeks", "*", "*"))
	if err != nil {
		t.Fatal(err)
	}
	enriched := 0
	for _, directory := range entries {
		indexPath := filepath.Join(directory, "enrichment.json")
		var index store.EnrichmentIndex
		if err := readJSONFile(indexPath, &index); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatal(err)
		}
		enriched++
		artifactPath := filepath.Join(
			directory, "enrichments", index.CurrentArtifact+".json",
		)
		var artifact store.EnrichmentRecord
		if err := readJSONFile(artifactPath, &artifact); err != nil {
			t.Fatal(err)
		}
		if artifact.ContentType != "text/plain" ||
			!strings.HasSuffix(artifact.Content, "complete body.") ||
			strings.Contains(artifact.Content, "script") {
			t.Fatalf("artifact = %+v", artifact)
		}
	}
	if enriched != 2 {
		t.Fatalf("enriched entries = %d, want 2", enriched)
	}

	second, err := collector.Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if second.Failed != 0 || second.Requests != 2 ||
		second.Feeds[0].Counts.NewEntries != 0 ||
		second.Feeds[0].Counts.Revisions != 0 ||
		second.Feeds[0].Counts.Enrichments != 0 {
		t.Fatalf("second report = %+v", second)
	}
}

func TestRunKeepsFeedWhenProviderEnrichmentFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/api" {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprint(w, `<?xml version="1.0"?><rss version="2.0"><channel>
			<title>Test</title><item><guid>one</guid><title>One</title>
			<description>Source excerpt.</description></item></channel></rss>`)
	}))
	defer server.Close()

	cfg := runnerConfig(t, server.URL+"/feed")
	cfg.Feeds[0].Enrichment = &config.Enrichment{
		Provider: "odata", URL: server.URL + "/api", MaxItemsPerRequest: 100,
	}
	report, err := New(cfg).Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed != 0 || report.Feeds[0].Status != "completed" ||
		report.Feeds[0].Counts.NewEntries != 1 ||
		report.Feeds[0].Counts.EnrichmentDeferred != 1 ||
		len(report.Feeds[0].Warnings) == 0 {
		t.Fatalf("report = %+v", report)
	}
	artifacts, err := filepath.Glob(filepath.Join(
		cfg.DataDir, "feeds", "test", "entries", "*", "enrichment.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 0 {
		t.Fatalf("enrichment artifacts = %v", artifacts)
	}
}

func TestRunRetriesMissingEnrichmentAfterNotModified(t *testing.T) {
	var apiCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api":
			if apiCalls.Add(1) == 1 {
				http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
			_, _ = fmt.Fprint(w, `{"value":[{"id":"one","description":"Complete body."}]}`)
		case "/feed":
			w.Header().Set("ETag", `"v1"`)
			if request.Header.Get("If-None-Match") == `"v1"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			_, _ = fmt.Fprint(w, `<?xml version="1.0"?><rss version="2.0"><channel>
				<title>Test</title><item><guid>one</guid><title>One</title>
				<link>/one</link><description>Source excerpt.</description></item>
				</channel></rss>`)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	cfg := runnerConfig(t, server.URL+"/feed")
	cfg.Feeds[0].Enrichment = &config.Enrichment{
		Provider: "odata", URL: server.URL + "/api",
		IDField: "id", ContentField: "description", ContentType: "text/html",
		MaxItemsPerRequest: 100,
	}
	collector := New(cfg)
	first, err := collector.Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if first.Failed != 0 || first.Feeds[0].Counts.EnrichmentDeferred != 1 {
		t.Fatalf("first report = %+v", first)
	}

	second, err := collector.Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if second.Failed != 0 || second.Requests != 2 ||
		!second.Feeds[0].NotModified ||
		second.Feeds[0].Counts.Enrichments != 1 ||
		second.Feeds[0].Counts.EnrichmentDeferred != 0 {
		t.Fatalf("second report = %+v", second)
	}
	if apiCalls.Load() != 2 {
		t.Fatalf("API calls = %d, want 2", apiCalls.Load())
	}
}

func TestRunReplaysLatestSnapshotForFullTextUpgrade(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		t.Error("full-text migration unexpectedly fetched the feed")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	now := time.Date(2026, 11, 22, 11, 0, 0, 0, time.UTC)
	longSummary := strings.Repeat("Complete summary. ", 30) + "Final summary sentence."
	longContent := strings.Repeat("Complete body. ", 80) + "Final body sentence."
	body := []byte(`<?xml version="1.0"?><rss version="2.0"
		xmlns:content="http://purl.org/rss/1.0/modules/content/"><channel><title>Upgrade</title>
		<item><guid>stable</guid><title>Upgrade entry</title><link>/upgrade</link>
		<description><![CDATA[<p>` + longSummary + `</p>]]></description>
		<content:encoded><![CDATA[<p>` + longContent + `</p>]]></content:encoded>
		</item></channel></rss>`)
	parsed, err := parse.Parse(body, server.URL+"/feed", "test", hashHex(body), now, 10)
	if err != nil {
		t.Fatal(err)
	}
	entry := parsed.Entries[0]

	cfg := runnerConfig(t, server.URL+"/feed")
	collector := New(cfg)
	collector.Now = func() time.Time { return now }
	old := entry
	old.Summary = "Truncated old summary..."
	old.Content = ""
	old.ContentHash = old.Record().Hash()
	if _, _, err := collector.Store.SaveEntry(old, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	metadata := store.SnapshotMetadata{
		SchemaVersion: 1, FeedID: "test",
		RequestedURL: server.URL + "/feed", EffectiveURL: server.URL + "/feed",
		Status: http.StatusOK, ContentType: "application/rss+xml",
		ETag: `"full"`, RetrievedAt: now.Add(-time.Minute).Format(time.RFC3339),
	}
	if err := collector.Store.SaveSnapshot("test", body, metadata); err != nil {
		t.Fatal(err)
	}
	if err := collector.Store.SaveState("test", store.State{
		SchemaVersion: 1, EntryStorageVersion: store.CompactEntryVersion - 1,
		ETag: `"full"`, LastSuccess: now.Format(time.RFC3339),
	}); err != nil {
		t.Fatal(err)
	}

	report, err := collector.Run(context.Background(), "", false)
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed != 0 || report.Requests != 0 ||
		report.Feeds[0].Status != "completed" ||
		report.Feeds[0].Counts.Revisions != 1 {
		t.Fatalf("migration report = %+v", report)
	}
	if requests.Load() != 0 {
		t.Fatalf("network requests = %d, want 0", requests.Load())
	}
	state, err := collector.Store.LoadState("test")
	if err != nil {
		t.Fatal(err)
	}
	if state.EntryStorageVersion != store.CompactEntryVersion ||
		state.PendingSnapshot != "" || state.ETag != `"full"` {
		t.Fatalf("state after migration = %+v", state)
	}
	base := entryDirectoryForKey(t, cfg.DataDir, "test", entry.EntryKey)
	var index store.EntryIndex
	if err := readJSONFile(filepath.Join(base, "index.json"), &index); err != nil {
		t.Fatal(err)
	}
	var record parse.Record
	if err := readJSONFile(filepath.Join(
		base, "revisions", index.CurrentRevision+".json",
	), &record); err != nil {
		t.Fatal(err)
	}
	if record.Summary != longSummary || record.Content != longContent {
		t.Fatalf("migrated record = %+v", record)
	}
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

func TestRunRecoversEntryWithoutIndexBeforeStorageUpgrade(t *testing.T) {
	body := []byte(`<?xml version="1.0"?><rss version="2.0"><channel><title>Pending</title>
		<item><guid>interrupted</guid><title>Interrupted</title>
		<description>Pending summary</description><link>/interrupted</link>
		<pubDate>Thu, 12 Nov 2026 16:00:02 +0000</pubDate></item>
		</channel></rss>`)
	feedURL := "https://example.com/feed"
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	parsed, err := parse.Parse(body, feedURL, "test", hashHex(body), now, 10)
	if err != nil {
		t.Fatal(err)
	}
	entry := parsed.Entries[0]
	legacyHash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	legacy := entry
	legacy.ContentHash = legacyHash

	collector := New(runnerConfig(t, feedURL))
	collector.Now = func() time.Time { return now }
	hash := hashHex(body)
	if err := collector.Store.SaveSnapshot("test", body, store.SnapshotMetadata{
		FeedID: "test", RequestedURL: feedURL, EffectiveURL: feedURL,
	}); err != nil {
		t.Fatal(err)
	}
	if err := collector.Store.SaveState("test", store.State{
		PendingSnapshot: hash, PendingEffectiveURL: feedURL,
		PendingRetrievedAt: now.Format(time.RFC3339), PendingContentType: "application/rss+xml",
	}); err != nil {
		t.Fatal(err)
	}
	revisions := filepath.Join(
		collector.Store.Root(), "feeds", "test", "entries", entry.EntryKey, "revisions",
	)
	if err := os.MkdirAll(revisions, 0o700); err != nil {
		t.Fatal(err)
	}
	legacyData, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	legacyPath := filepath.Join(revisions, legacyHash+".json")
	if err := os.WriteFile(legacyPath, legacyData, 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := collector.Run(context.Background(), "", false)
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed != 0 || report.Feeds[0].Status != "completed" {
		t.Fatalf("report = %+v", report)
	}
	state, err := collector.Store.LoadState("test")
	if err != nil {
		t.Fatal(err)
	}
	if state.EntryStorageVersion != store.CompactEntryVersion || state.PendingSnapshot != "" {
		t.Fatalf("state after recovery = %+v", state)
	}
	index := filepath.Join(
		entryDirectoryForKey(t, collector.Store.Root(), "test", entry.EntryKey), "index.json",
	)
	if _, err := os.Stat(index); err != nil {
		t.Fatalf("recovered index missing: %v", err)
	}
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("legacy revision still exists: %v", err)
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

func TestRunRefetchesRejectedSnapshotOnSchedule(t *testing.T) {
	const (
		feedBody         = `<rss version="2.0"><channel><title>Feed</title><item><guid>stable</guid><title>%s</title></item></channel></rss>`
		goodETag         = `"last-good"`
		goodLastModified = "Mon, 06 Jan 2025 12:00:00 GMT"
	)
	for _, tt := range []struct {
		name, body, encoding, failureClass string
	}{
		{name: "malformed RSS", body: `<rss><channel><item>`, failureClass: "parse"},
		{name: "invalid gzip", body: "not a gzip stream", encoding: "gzip", failureClass: "content"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				n := requests.Add(1)
				switch n {
				case 1:
					w.Header().Set("ETag", goodETag)
					w.Header().Set("Last-Modified", goodLastModified)
					_, _ = fmt.Fprintf(w, feedBody, "Before")
					return
				case 2, 3:
					w.Header().Set("ETag", `"rejected"`)
					w.Header().Set("Last-Modified", "Mon, 06 Jan 2025 12:01:00 GMT")
					if tt.encoding != "" {
						w.Header().Set("Content-Encoding", tt.encoding)
					}
				default:
					w.Header().Set("ETag", `"recovered"`)
				}
				if got := req.Header.Get("If-None-Match"); got != goodETag {
					t.Errorf("If-None-Match = %q, want %q", got, goodETag)
				}
				if got := req.Header.Get("If-Modified-Since"); got != goodLastModified {
					t.Errorf("If-Modified-Since = %q, want %q", got, goodLastModified)
				}
				if n <= 3 {
					_, _ = fmt.Fprint(w, tt.body)
				} else {
					_, _ = fmt.Fprintf(w, feedBody, "After")
				}
			}))
			defer server.Close()

			collector := New(runnerConfig(t, server.URL))
			now := time.Date(2025, 1, 6, 13, 0, 0, 0, time.UTC)
			collector.Now = func() time.Time { return now }
			run := func(force bool) *Report {
				t.Helper()
				report, err := collector.Run(context.Background(), "", force)
				if err != nil || len(report.Feeds) != 1 {
					t.Fatalf("run: report = %+v, error = %v", report, err)
				}
				return report
			}
			loadState := func() store.State {
				t.Helper()
				state, err := collector.Store.LoadState("test")
				if err != nil {
					t.Fatal(err)
				}
				return state
			}
			first := run(false)
			if first.Failed != 0 || first.Feeds[0].Counts.NewEntries != 1 {
				t.Fatalf("initial run = %+v", first)
			}

			now = now.Add(time.Second)
			firstFailure := run(true)
			if firstFailure.Failed != 1 || firstFailure.Requests != 1 || firstFailure.Feeds[0].FailureClass != tt.failureClass {
				t.Errorf("first failure = %+v", firstFailure)
			}
			hash := firstFailure.Feeds[0].Snapshot
			archived, err := collector.Store.ReadSnapshot("test", hash)
			if err != nil || string(archived) != tt.body {
				t.Errorf("archived snapshot = %q, error = %v", archived, err)
			}
			state := loadState()
			if state.PendingSnapshot != "" || state.PendingETag != "" || state.PendingLastMod != "" ||
				state.ETag != goodETag || state.LastModified != goodLastModified ||
				state.ConsecutiveErrors != 1 || state.NextRetryAt != now.Add(time.Second).Format(time.RFC3339) {
				t.Errorf("state after first rejection = %+v", state)
			}
			now = now.Add(500 * time.Millisecond)
			if early := run(false); early.Requests != 0 || early.Feeds[0].Status != "skipped" {
				t.Errorf("run before retry due = %+v", early)
			}

			now = now.Add(500 * time.Millisecond)
			secondFailure := run(false)
			state = loadState()
			if secondFailure.Failed != 1 || secondFailure.Requests != 1 || state.ConsecutiveErrors != 2 ||
				state.NextRetryAt != now.Add(2*time.Second).Format(time.RFC3339) {
				t.Errorf("second failure: report = %+v, state = %+v", secondFailure, state)
			}

			now = now.Add(2 * time.Second)
			recovered := run(false)
			state = loadState()
			if recovered.Failed != 0 || recovered.Requests != 1 || recovered.Feeds[0].Counts.Revisions != 1 ||
				requests.Load() != 4 || state.ETag != `"recovered"` || state.NextRetryAt != "" || state.ConsecutiveErrors != 0 {
				t.Errorf("recovery: report = %+v, state = %+v, network requests = %d", recovered, state, requests.Load())
			}
		})
	}
}

func TestRunRetainsCorruptPendingSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("a corrupt pending snapshot should not trigger a network request")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	feedURL := server.URL
	collector := New(runnerConfig(t, feedURL))
	body := []byte(`<rss version="2.0"><channel><title>Test</title><item><guid>one</guid></item></channel></rss>`)
	hash := hashHex(body)
	if err := collector.Store.SaveSnapshot("test", body, store.SnapshotMetadata{
		FeedID: "test", RequestedURL: feedURL, EffectiveURL: feedURL,
	}); err != nil {
		t.Fatal(err)
	}
	if err := collector.Store.SaveState("test", store.State{
		PendingSnapshot: hash, PendingETag: `"pending"`, PendingEffectiveURL: feedURL,
	}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(collector.Store.Root(), "feeds", "test", "snapshots", hash+".xml")
	if err := os.WriteFile(path, []byte(`<rss><channel>`), 0o600); err != nil {
		t.Fatal(err)
	}
	report, err := collector.Run(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed != 1 || report.Requests != 0 || report.Feeds[0].FailureClass != "storage" || report.Feeds[0].Snapshot != hash {
		t.Errorf("corrupt replay report = %+v", report)
	}
	state, err := collector.Store.LoadState("test")
	if err != nil {
		t.Fatal(err)
	}
	if state.PendingSnapshot != hash || state.PendingETag != `"pending"` || state.ETag != "" {
		t.Errorf("state after corrupted replay = %+v", state)
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

func entryDirectoryForKey(t *testing.T, dataDir, feedID, entryKey string) string {
	t.Helper()
	var location store.EntryLocation
	if err := readJSONFile(filepath.Join(
		dataDir, "feeds", feedID, "locations", entryKey+".json",
	), &location); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(
		dataDir, "feeds", feedID, "weeks", location.CanonicalWeek, entryKey,
	)
}

func readJSONFile(path string, target any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
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
