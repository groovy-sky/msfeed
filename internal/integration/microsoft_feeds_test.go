package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/groovy-sky/msfeed/internal/config"
	"github.com/groovy-sky/msfeed/internal/parse"
	"github.com/groovy-sky/msfeed/internal/runner"
	"github.com/groovy-sky/msfeed/internal/store"
)

const microsoftConfig = "../../configs/microsoft-feeds.json"
const microsoftFixtures = "../../testdata/feeds/microsoft"

type expectedFixture struct {
	SchemaVersion int             `json:"schema_version"`
	FeedID        string          `json:"feed_id"`
	Format        string          `json:"format"`
	EntryCount    int             `json:"entry_count"`
	Entries       []expectedEntry `json:"entries"`
}

type expectedEntry struct {
	IdentityBasis string `json:"identity_basis"`
	Title         string `json:"title"`
	URL           string `json:"url"`
	EntryKey      string `json:"entry_key"`
	ContentHash   string `json:"content_hash"`
}

type captureRecord struct {
	SchemaVersion      int      `json:"schema_version"`
	FeedID             string   `json:"feed_id"`
	SourceKind         string   `json:"source_kind"`
	RequestedURL       string   `json:"requested_url"`
	EffectiveURL       string   `json:"effective_url,omitempty"`
	CapturedAt         string   `json:"captured_at,omitempty"`
	HTTPStatus         *int     `json:"http_status,omitempty"`
	RedirectChain      []string `json:"redirect_chain,omitempty"`
	ContentType        string   `json:"content_type,omitempty"`
	ContentEncoding    string   `json:"content_encoding,omitempty"`
	ETag               string   `json:"etag,omitempty"`
	LastModified       string   `json:"last_modified,omitempty"`
	BodySHA256         string   `json:"body_sha256"`
	CaptureToolVersion string   `json:"capture_tool_version"`
	ParseWarnings      []string `json:"parse_warnings"`
	Verification       string   `json:"verification"`
	Limitations        []string `json:"limitations"`
}

type compatibilityResult struct {
	FeedID              string   `json:"feed_id"`
	RequestedURL        string   `json:"requested_url"`
	EffectiveURL        string   `json:"effective_url,omitempty"`
	CheckedAt           string   `json:"checked_at"`
	TransportResult     string   `json:"transport_result"`
	TLSResult           string   `json:"tls_result"`
	HTTPStatus          int      `json:"http_status,omitempty"`
	RedirectChain       []string `json:"redirect_chain,omitempty"`
	SnapshotHash        string   `json:"snapshot_sha256,omitempty"`
	ParseResult         string   `json:"parse_result"`
	DetectedFormat      string   `json:"detected_format,omitempty"`
	EntryCount          int      `json:"entry_count"`
	NormalizationResult string   `json:"normalization_result"`
	EntryChecks         string   `json:"entry_checks"`
	IdempotencyResult   string   `json:"idempotency_result"`
	ConditionalFetch    string   `json:"conditional_fetch_result"`
	Warnings            []string `json:"warnings"`
	FailureClass        string   `json:"failure_class,omitempty"`
	FailureDetail       string   `json:"failure_detail,omitempty"`
	Overall             string   `json:"overall"`
}

func TestMicrosoftRepresentativeFixtures(t *testing.T) {
	cfg, err := config.Load(microsoftConfig)
	if err != nil {
		t.Fatal(err)
	}
	for _, feed := range cfg.Feeds {
		feed := feed
		t.Run(feed.ID, func(t *testing.T) {
			dir := filepath.Join(microsoftFixtures, feed.ID)
			body, err := os.ReadFile(filepath.Join(dir, "original.xml"))
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := parse.Parse(body, feed.URL, feed.ID, "representative-fixture", time.Unix(0, 0), feed.MaxEntriesPerRun)
			if err != nil {
				t.Fatal(err)
			}
			expected := expectedFromParsed(feed.ID, parsed)
			capture := representativeCapture(feed, body)

			if os.Getenv("RSSCOLLECTOR_UPDATE_FIXTURES") == "1" {
				writeFixtureJSON(t, filepath.Join(dir, "expected.json"), expected)
				writeFixtureJSON(t, filepath.Join(dir, "capture.json"), capture)
			}
			var want expectedFixture
			readFixtureJSON(t, filepath.Join(dir, "expected.json"), &want)
			var storedCapture captureRecord
			readFixtureJSON(t, filepath.Join(dir, "capture.json"), &storedCapture)
			if !reflect.DeepEqual(expected, want) {
				t.Fatalf("fixture result differs from expected\nwant: %#v\ngot:  %#v", want, expected)
			}
			if storedCapture.BodySHA256 != capture.BodySHA256 {
				t.Fatalf("capture hash = %s, want %s", storedCapture.BodySHA256, capture.BodySHA256)
			}
			if storedCapture.Verification != "UNVERIFIED" || storedCapture.SourceKind != "representative" {
				t.Fatalf("fixture must remain explicitly unverified: %#v", storedCapture)
			}

			verifyReorderIdentity(t, body, parsed, feed)
			verifyRevisionAndReplay(t, body, parsed, feed)
		})
	}
}

func TestMicrosoftFeeds(t *testing.T) {
	if os.Getenv("RSSCOLLECTOR_LIVE_TESTS") != "1" {
		t.Skip("set RSSCOLLECTOR_LIVE_TESTS=1 for live Microsoft feed compatibility")
	}
	base, err := config.Load(microsoftConfig)
	if err != nil {
		t.Fatal(err)
	}
	results := make([]compatibilityResult, 0, len(base.Feeds))
	for _, configured := range base.Feeds {
		configured := configured
		t.Run(configured.ID, func(t *testing.T) {
			result := runLiveCheck(t, *base, configured)
			results = append(results, result)
			if result.Overall != "PASS" {
				t.Errorf("%s compatibility is %s: %s", configured.ID, result.Overall, result.FailureClass)
			}
		})
	}
	writeLiveReports(t, results)
}

func expectedFromParsed(feedID string, parsed *parse.Feed) expectedFixture {
	expected := expectedFixture{
		SchemaVersion: 1, FeedID: feedID, Format: parsed.Format, EntryCount: len(parsed.Entries),
		Entries: make([]expectedEntry, 0, len(parsed.Entries)),
	}
	for _, entry := range parsed.Entries {
		expected.Entries = append(expected.Entries, expectedEntry{
			IdentityBasis: entry.IdentityBasis, Title: entry.Title, URL: entry.URL,
			EntryKey: entry.EntryKey, ContentHash: entry.ContentHash,
		})
	}
	return expected
}

func representativeCapture(feed config.Feed, body []byte) captureRecord {
	return captureRecord{
		SchemaVersion: 1, FeedID: feed.ID, SourceKind: "representative",
		RequestedURL: feed.URL, BodySHA256: hashBytes(body),
		CaptureToolVersion: "rsscollector-fixture-v1",
		ParseWarnings:      []string{},
		Verification:       "UNVERIFIED",
		Limitations: []string{
			"not a capture from the configured live endpoint",
			"live parsing and endpoint availability are not established",
		},
	}
}

func verifyReorderIdentity(t *testing.T, body []byte, original *parse.Feed, feed config.Feed) {
	t.Helper()
	reordered := reverseEntryElements(body)
	parsed, err := parse.Parse(reordered, feed.URL, feed.ID, "reordered", time.Unix(0, 0), feed.MaxEntriesPerRun)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Entries) != len(original.Entries) {
		t.Fatalf("reordered entries = %d, want %d", len(parsed.Entries), len(original.Entries))
	}
	hashes := make(map[string]string, len(original.Entries))
	for _, entry := range original.Entries {
		hashes[entry.EntryKey] = entry.ContentHash
	}
	for _, entry := range parsed.Entries {
		if hashes[entry.EntryKey] != entry.ContentHash {
			t.Fatalf("reordered identity/hash mismatch for %s", entry.EntryKey)
		}
	}
}

func verifyRevisionAndReplay(t *testing.T, body []byte, original *parse.Feed, feed config.Feed) {
	t.Helper()
	dataDir := t.TempDir()
	st := store.New(dataDir)
	if err := st.Initialize(); err != nil {
		t.Fatal(err)
	}
	observed := time.Unix(100, 0).UTC()
	for _, entry := range original.Entries {
		if newEntry, _, err := st.SaveEntry(entry, observed); err != nil || !newEntry {
			t.Fatalf("save initial entry: new=%v err=%v", newEntry, err)
		}
		if newEntry, changed, err := st.SaveEntry(entry, observed.Add(time.Minute)); err != nil || newEntry || changed {
			t.Fatalf("replay entry: new=%v changed=%v err=%v", newEntry, changed, err)
		}
	}

	edited := bytes.Replace(body, []byte("Original"), []byte("Edited"), 1)
	changed, err := parse.Parse(edited, feed.URL, feed.ID, "edited", observed, feed.MaxEntriesPerRun)
	if err != nil {
		t.Fatal(err)
	}
	originalByKey := make(map[string]parse.Entry, len(original.Entries))
	for _, entry := range original.Entries {
		originalByKey[entry.EntryKey] = entry
	}
	changedEntryFound := false
	for _, entry := range changed.Entries {
		previous, ok := originalByKey[entry.EntryKey]
		if !ok || previous.ContentHash == entry.ContentHash {
			continue
		}
		changedEntryFound = true
		if newEntry, revision, err := st.SaveEntry(entry, observed.Add(2*time.Minute)); err != nil || newEntry || !revision {
			t.Fatalf("save changed entry: new=%v revision=%v err=%v", newEntry, revision, err)
		}
		break
	}
	if !changedEntryFound {
		t.Fatal("edited fixture did not preserve entry identity and create a revision")
	}

	interruptedDir := t.TempDir()
	interrupted := store.New(interruptedDir)
	if err := interrupted.Initialize(); err != nil {
		t.Fatal(err)
	}
	metadata := store.SnapshotMetadata{
		SchemaVersion: 1, FeedID: feed.ID, RequestedURL: feed.URL,
		EffectiveURL: feed.URL, Status: 200, RetrievedAt: observed.Format(time.RFC3339),
	}
	if err := interrupted.SaveSnapshot(feed.ID, body, metadata); err != nil {
		t.Fatal(err)
	}
	state := store.State{
		SchemaVersion: 1, PendingSnapshot: hashBytes(body), PendingEffectiveURL: feed.URL,
		PendingRetrievedAt: observed.Format(time.RFC3339),
	}
	if err := interrupted.SaveState(feed.ID, state); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		SchemaVersion: 1, DataDir: interruptedDir, MaxRunSeconds: 10,
		HTTP:     config.HTTP{TimeoutSeconds: 2, MaxBodyBytes: 1 << 20, MaxRedirects: 1, MaxRequestsPerRun: 5},
		Retry:    config.Retry{MaxAttempts: 3, InitialBackoffSeconds: 1, MaxBackoffSeconds: 10},
		Defaults: config.Defaults{PollIntervalSeconds: 1, MaxEntriesPerRun: feed.MaxEntriesPerRun},
		Feeds: []config.Feed{{
			ID: feed.ID, URL: feed.URL, Enabled: true,
			PollIntervalSeconds: 1, MaxEntriesPerRun: feed.MaxEntriesPerRun,
		}},
	}
	report, err := runner.New(cfg).Run(context.Background(), feed.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed != 0 || report.Requests != 0 || report.Feeds[0].Counts.EntriesSeen != original.TotalEntries {
		t.Fatalf("pending replay report = %+v", report)
	}
}

func runLiveCheck(t *testing.T, base config.Config, feed config.Feed) compatibilityResult {
	t.Helper()
	checkedAt := time.Now().UTC().Format(time.RFC3339)
	result := compatibilityResult{
		FeedID: feed.ID, RequestedURL: feed.URL, CheckedAt: checkedAt,
		TransportResult: "UNVERIFIED", TLSResult: "NOT_EXERCISED", ParseResult: "UNVERIFIED",
		NormalizationResult: "UNVERIFIED", EntryChecks: "NOT_EXERCISED",
		IdempotencyResult: "UNVERIFIED", ConditionalFetch: "NOT_EXERCISED",
		Warnings: []string{}, Overall: "UNVERIFIED",
	}
	cfg := base
	cfg.DataDir = t.TempDir()
	if captureDir := os.Getenv("RSSCOLLECTOR_LIVE_CAPTURE_DIR"); captureDir != "" {
		stamp := strings.ReplaceAll(checkedAt, ":", "")
		cfg.DataDir = filepath.Join(captureDir, stamp, feed.ID)
	}
	defer func() {
		writeLiveCapture(t, cfg.DataDir, feed, result)
	}()
	cfg.MaxFeedsPerRun = 1
	cfg.Feeds = []config.Feed{feed}
	collector := runner.New(&cfg)
	report, err := collector.Run(context.Background(), feed.ID, true)
	if err != nil {
		result.FailureClass = "runner"
		result.Warnings = append(result.Warnings, err.Error())
		return result
	}

	if len(report.Feeds) == 0 {
		result.FailureClass = "runner"
		return result
	}
	first := report.Feeds[0]
	result.HTTPStatus = first.HTTPStatus
	result.EffectiveURL = first.EffectiveURL
	result.RedirectChain = append([]string{}, first.Redirects...)
	result.SnapshotHash = first.Snapshot
	result.DetectedFormat = first.Format
	result.EntryCount = first.Counts.EntriesSeen
	result.Warnings = append(result.Warnings, first.Warnings...)
	if first.Status == "failed" {
		result.FailureClass = first.FailureClass
		result.FailureDetail = first.Error
		result.Warnings = append(result.Warnings, first.Error)
		if first.FailureClass == "parse" || first.FailureClass == "content" {
			result.ParseResult = "FAIL"
			result.Overall = "FAIL"
		}
		return result
	}
	result.TransportResult = "PASS"
	if strings.HasPrefix(strings.ToLower(feed.URL), "https://") {
		result.TLSResult = "PASS"
	}
	result.ParseResult = "PASS"
	result.NormalizationResult = "PASS"
	if first.Counts.EntriesSeen > 0 {
		result.EntryChecks = "PASS"
	}

	for attempts := 0; first.Status == "partial" && attempts < 20; attempts++ {
		next, err := collector.Run(context.Background(), feed.ID, false)
		if err != nil {
			result.FailureClass = "recovery"
			result.Overall = "FAIL"
			return result
		}
		if len(next.Feeds) == 0 || next.Feeds[0].Status == "failed" {
			result.FailureClass = "recovery"
			result.Overall = "FAIL"
			return result
		}
		result.EntryCount += next.Feeds[0].Counts.EntriesSeen
		first = next.Feeds[0]
	}
	if first.Status == "partial" {
		result.FailureClass = "entry_budget"
		return result
	}

	replay, err := collector.Run(context.Background(), feed.ID, true)
	if err != nil {
		result.FailureClass = "idempotency"
		result.Overall = "FAIL"
		return result
	}
	if replay.Failed != 0 || len(replay.Feeds) == 0 {
		result.FailureClass = "idempotency"
		result.Overall = "FAIL"
		return result
	}
	replayFeed := replay.Feeds[0]
	replayNew := replayFeed.Counts.NewEntries
	replayRevisions := replayFeed.Counts.Revisions
	conditional := replayFeed.Conditional
	for attempts := 0; replayFeed.Status == "partial" && attempts < 20; attempts++ {
		next, err := collector.Run(context.Background(), feed.ID, false)
		if err != nil || len(next.Feeds) == 0 || next.Feeds[0].Status == "failed" {
			result.FailureClass = "idempotency"
			result.Overall = "FAIL"
			return result
		}
		replayNew += next.Feeds[0].Counts.NewEntries
		replayRevisions += next.Feeds[0].Counts.Revisions
		replayFeed = next.Feeds[0]
	}
	if replayFeed.Status == "partial" {
		result.FailureClass = "entry_budget"
		result.Overall = "UNVERIFIED"
		return result
	}
	if replayNew == 0 && replayRevisions == 0 {
		result.IdempotencyResult = "PASS"
	} else {
		result.IdempotencyResult = "UNVERIFIED"
		result.FailureClass = "feed_changed_during_check"
		return result
	}
	switch conditional {
	case "not_modified":
		result.ConditionalFetch = "PASS"
	case "sent":
		result.ConditionalFetch = "PASS"
	case "not_sent":
		result.ConditionalFetch = "NOT_EXERCISED"
		result.Warnings = append(result.Warnings, "no validators were supplied by the feed")
	}
	if result.IdempotencyResult == "PASS" && result.ParseResult == "PASS" {
		result.Overall = "PASS"
	}
	return result
}

func writeLiveCapture(t *testing.T, dataDir string, feed config.Feed, result compatibilityResult) {
	t.Helper()
	if result.SnapshotHash == "" {
		return
	}
	metadataPath := filepath.Join(
		dataDir, "feeds", feed.ID, "snapshot-metadata", result.SnapshotHash+".json",
	)
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Errorf("read live snapshot metadata: %v", err)
		return
	}
	var metadata store.SnapshotMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		t.Errorf("decode live snapshot metadata: %v", err)
		return
	}
	status := metadata.Status
	record := captureRecord{
		SchemaVersion: 1, FeedID: feed.ID, SourceKind: "live",
		RequestedURL: metadata.RequestedURL, EffectiveURL: metadata.EffectiveURL,
		CapturedAt: metadata.RetrievedAt, HTTPStatus: &status,
		RedirectChain: metadata.Redirects, ContentType: metadata.ContentType,
		ContentEncoding: metadata.ContentEncoding, ETag: metadata.ETag,
		LastModified: metadata.LastModified, BodySHA256: metadata.Hash,
		CaptureToolVersion: "rsscollector-live-test-v1",
		ParseWarnings:      append([]string{}, result.Warnings...), Verification: result.Overall,
		Limitations: []string{"raw response is retained only in RSSCOLLECTOR_LIVE_CAPTURE_DIR"},
	}
	writeFixtureJSON(t, filepath.Join(dataDir, "capture.json"), record)
}

func writeLiveReports(t *testing.T, results []compatibilityResult) {
	t.Helper()
	outputDir := os.Getenv("RSSCOLLECTOR_REPORT_DIR")
	if outputDir == "" {
		return
	}
	if err := os.MkdirAll(outputDir, 0o700); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC().Format("20060102T150405Z")
	encoded, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(filepath.Join(outputDir, "microsoft-compatibility-"+stamp+".json"), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	fmt.Fprintln(&text, "Microsoft feed compatibility")
	fmt.Fprintln(&text, "feed_id\toverall\ttransport\ttls\tparse\tidempotency\tconditional\tfailure")
	for _, result := range results {
		line := fmt.Sprintf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
			result.FeedID, result.Overall, result.TransportResult, result.TLSResult, result.ParseResult,
			result.IdempotencyResult, result.ConditionalFetch, result.FailureClass)
		fmt.Fprintln(&text, strings.TrimRight(line, "\t"))
	}
	if err := os.WriteFile(filepath.Join(outputDir, "microsoft-compatibility-"+stamp+".txt"), []byte(text.String()), 0o600); err != nil {
		t.Fatal(err)
	}
}

func reverseEntryElements(body []byte) []byte {
	expression := regexp.MustCompile(`(?is)<(item|entry)(?:\s[^>]*)?>.*?</(?:item|entry)>`)
	matches := expression.FindAllIndex(body, -1)
	if len(matches) < 2 {
		return body
	}
	var output bytes.Buffer
	position := 0
	for index, match := range matches {
		output.Write(body[position:match[0]])
		replacement := matches[len(matches)-1-index]
		output.Write(body[replacement[0]:replacement[1]])
		position = match[1]
	}
	output.Write(body[position:])
	return output.Bytes()
}

func hashBytes(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func writeFixtureJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readFixtureJSON(t *testing.T, path string, target any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		t.Fatal(err)
	}
}
