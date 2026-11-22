package runner

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"mime"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/groovy-sky/msfeed/internal/config"
	"github.com/groovy-sky/msfeed/internal/fetch"
	"github.com/groovy-sky/msfeed/internal/parse"
	"github.com/groovy-sky/msfeed/internal/store"
)

var ErrLocked = errors.New("another collector process holds the data directory lock")
var ErrStorage = errors.New("collector storage failure")

type Report struct {
	SchemaVersion int          `json:"schema_version"`
	RunID         string       `json:"run_id"`
	StartedAt     string       `json:"started_at"`
	FinishedAt    string       `json:"finished_at"`
	Feeds         []FeedReport `json:"feeds"`
	Requests      int          `json:"requests"`
	Failed        int          `json:"failed"`
	BudgetReached bool         `json:"budget_reached,omitempty"`
}

type FeedReport struct {
	FeedID           string           `json:"feed_id"`
	RequestedURL     string           `json:"requested_url,omitempty"`
	Status           string           `json:"status"`
	Format           string           `json:"format,omitempty"`
	Snapshot         string           `json:"snapshot,omitempty"`
	EffectiveURL     string           `json:"effective_url,omitempty"`
	HTTPStatus       int              `json:"http_status,omitempty"`
	Redirects        []string         `json:"redirects,omitempty"`
	Counts           store.FeedCounts `json:"counts"`
	RemainingEntries int              `json:"remaining_entries,omitempty"`
	NotModified      bool             `json:"not_modified,omitempty"`
	Conditional      string           `json:"conditional_fetch,omitempty"`
	FailureClass     string           `json:"failure_class,omitempty"`
	Warnings         []string         `json:"warnings,omitempty"`
	Error            string           `json:"error,omitempty"`
}

type Runner struct {
	Config *config.Config
	Store  *store.Store
	Now    func() time.Time
}

func New(cfg *config.Config) *Runner {
	if cfg.HTTP.MaxRequestsPerRun == 0 {
		cfg.HTTP.MaxRequestsPerRun = 100
	}
	if cfg.Retry.MaxAttempts == 0 {
		cfg.Retry.MaxAttempts = 5
	}
	if cfg.Retry.InitialBackoffSeconds == 0 {
		cfg.Retry.InitialBackoffSeconds = 30
	}
	if cfg.Retry.MaxBackoffSeconds == 0 {
		cfg.Retry.MaxBackoffSeconds = 3600
	}
	return &Runner{Config: cfg, Store: store.New(cfg.DataDir), Now: time.Now}
}

func (r *Runner) Run(ctx context.Context, onlyFeed string, force bool) (*Report, error) {
	if err := r.Store.Initialize(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStorage, err)
	}
	lock, err := acquireLock(filepath.Join(r.Store.Root(), "collector.lock"))
	if err != nil {
		if errors.Is(err, ErrLocked) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", ErrStorage, err)
	}
	defer releaseLock(lock)

	start := r.Now()
	runID := start.UTC().Format("20060102T150405.000000000Z")
	report := &Report{SchemaVersion: 1, RunID: runID, StartedAt: start.UTC().Format(time.RFC3339Nano)}
	budget, cancel := context.WithTimeout(ctx, r.Config.RunTimeout())
	defer cancel()

	selected := false
	processed := 0
	for _, feed := range r.Config.Feeds {
		if !feed.Enabled || (onlyFeed != "" && feed.ID != onlyFeed) {
			continue
		}
		selected = true
		if r.Config.MaxFeedsPerRun > 0 && processed >= r.Config.MaxFeedsPerRun {
			report.BudgetReached = true
			break
		}
		processed++
		if err := r.Store.SaveFeed(store.FeedConfig{
			SchemaVersion: 1, ID: feed.ID, URL: feed.URL,
		}); err != nil {
			report.Feeds = append(report.Feeds, FeedReport{
				FeedID: feed.ID, RequestedURL: feed.URL, Status: "failed",
				FailureClass: "storage", Error: err.Error(),
			})
			report.Failed++
			continue
		}
		if err := budget.Err(); err != nil {
			report.BudgetReached = true
			break
		}

		feedReport, requests, budgetReached := r.processFeed(
			budget, feed, force, r.Config.HTTP.MaxRequestsPerRun-report.Requests,
		)
		report.Feeds = append(report.Feeds, feedReport)
		report.Requests += requests
		if feedReport.Status == "failed" {
			report.Failed++
		}
		if budgetReached {
			report.BudgetReached = true
		}
	}
	if onlyFeed != "" && !selected {
		return nil, fmt.Errorf("feed %q is not enabled or does not exist", onlyFeed)
	}
	report.FinishedAt = r.Now().UTC().Format(time.RFC3339Nano)
	if budget.Err() != nil && !errors.Is(budget.Err(), context.Canceled) {
		report.BudgetReached = true
	}
	if err := r.Store.SaveRun(runID, report); err != nil {
		return report, fmt.Errorf("%w: save run report: %v", ErrStorage, err)
	}
	return report, nil
}

func (r *Runner) processFeed(ctx context.Context, feed config.Feed, force bool, requestBudget int) (FeedReport, int, bool) {
	report := FeedReport{FeedID: feed.ID, RequestedURL: feed.URL, Status: "completed"}
	state, err := r.Store.LoadState(feed.ID)
	if err != nil {
		report.Status = "failed"
		report.FailureClass = "storage"
		report.Error = fmt.Sprintf("load state: %v", err)
		return report, 0, false
	}
	now := r.Now()
	state.SchemaVersion = 1
	if state.EntryStorageVersion > store.CompactEntryVersion {
		report.Status = "failed"
		report.FailureClass = "storage"
		report.Error = fmt.Sprintf(
			"entry storage version %d is newer than supported version %d",
			state.EntryStorageVersion, store.CompactEntryVersion,
		)
		return report, 0, false
	}
	storageReady := state.EntryStorageVersion == store.CompactEntryVersion
	if state.EntryStorageVersion < store.CompactEntryVersion {
		ready, budgetReached := r.upgradeEntryStorage(ctx, feed, &state, now, &report)
		if report.Status == "failed" {
			return report, 0, budgetReached
		}
		storageReady = ready
	}
	state.LastAttempt = now.UTC().Format(time.RFC3339)
	if state.PendingSnapshot != "" {
		if !force && retryNotDue(state, now) {
			report.Status = "skipped"
			return report, 0, false
		}
		pending, budgetReached := r.processPending(ctx, feed, state, report, now)
		if !storageReady && (pending.Status == "completed" || pending.Status == "partial") {
			upgradeState, budgetUsed := r.finishEntryStorageUpgrade(ctx, feed, now, &pending)
			pending = upgradeState
			budgetReached = budgetReached || budgetUsed
		}
		return pending, 0, budgetReached
	}
	if !force && !due(state, now, feed.PollIntervalSeconds) {
		report.Status = "skipped"
		return report, 0, false
	}
	if requestBudget <= 0 {
		report.Status = "deferred"
		report.FailureClass = "budget"
		return report, 0, true
	}

	headers, err := r.Config.ResolveAuthentication(feed)
	if err != nil {
		report.Status = "failed"
		report.FailureClass = "configuration"
		report.Error = err.Error()
		r.recordFailure(&report, feed, &state, now, err)
		return report, 0, false
	}
	maxRedirects := r.Config.HTTP.MaxRedirects
	if allowed := requestBudget - 1; maxRedirects > allowed {
		maxRedirects = allowed
	}
	result, err := fetch.One(ctx, fetch.Request{
		URL: feed.URL, ETag: state.ETag, LastModified: state.LastModified,
		UserAgent: r.Config.HTTP.UserAgent, Timeout: r.Config.HTTPTimeout(),
		MaxBodyBytes: r.Config.HTTP.MaxBodyBytes, MaxRedirects: maxRedirects,
		AllowPrivateNetwork: r.Config.HTTP.AllowPrivateNetworks, PublicDNSFallback: r.Config.HTTP.PublicDNSFallback,
		Headers: headers, Now: r.Now,
	})
	requests := 1 + len(result.Redirects)
	report.EffectiveURL = result.EffectiveURL
	report.HTTPStatus = result.Status
	report.Redirects = append([]string{}, result.Redirects...)
	report.Conditional = "not_sent"
	if state.ETag != "" || state.LastModified != "" {
		report.Conditional = "sent"
	}
	// A complete body with a decoding error still needs to be archived.
	if err != nil && result.Body == nil {
		report.Status = "failed"
		report.FailureClass = failureClass(err)
		report.Error = err.Error()
		r.recordFailure(&report, feed, &state, now, err)
		return report, requests, errors.Is(ctx.Err(), context.DeadlineExceeded)
	}
	if result.NotModified {
		report.NotModified = true
		report.Conditional = "not_modified"
		state.LastSuccess = now.UTC().Format(time.RFC3339)
		clearFailure(&state)
		if err := r.Store.SaveState(feed.ID, state); err != nil {
			report.Status = "failed"
			report.FailureClass = "storage"
			report.Error = err.Error()
		}
		return report, requests, false
	}

	lowDisk, err := r.Store.CheckCapacity(int64(len(result.Body)))
	if err != nil {
		report.Status = "failed"
		report.FailureClass = "storage"
		report.Error = err.Error()
		r.recordFailure(&report, feed, &state, now, err)
		return report, requests, false
	}
	if lowDisk {
		report.Warnings = append(report.Warnings, "available disk space is below 64 MiB")
	}
	if err := r.persistFetched(feed, &state, result, now); err != nil {
		report.Status = "failed"
		report.FailureClass = "storage"
		report.Error = err.Error()
		r.recordFailure(&report, feed, &state, now, err)
		return report, requests, false
	}
	pending, budgetReached := r.processPending(ctx, feed, state, report, now)
	return pending, requests, budgetReached
}

func (r *Runner) upgradeEntryStorage(
	ctx context.Context,
	feed config.Feed,
	state *store.State,
	now time.Time,
	report *FeedReport,
) (bool, bool) {
	complete, err := r.Store.UpgradeEntryStorage(
		ctx, feed.ID, state.PendingSnapshot != "",
	)
	if err != nil {
		return false, r.failEntryStorageUpgrade(ctx, feed, state, now, report, err)
	}
	if !complete {
		return false, false
	}
	state.EntryStorageVersion = store.CompactEntryVersion
	if err := r.Store.SaveState(feed.ID, *state); err != nil {
		return false, r.failEntryStorageUpgrade(ctx, feed, state, now, report, err)
	}
	return true, false
}

func (r *Runner) finishEntryStorageUpgrade(
	ctx context.Context,
	feed config.Feed,
	now time.Time,
	report *FeedReport,
) (FeedReport, bool) {
	state, err := r.Store.LoadState(feed.ID)
	if err != nil {
		report.Status = "failed"
		report.FailureClass = "storage"
		report.Error = fmt.Sprintf("load state after pending replay: %v", err)
		return *report, false
	}
	_, budgetReached := r.upgradeEntryStorage(ctx, feed, &state, now, report)
	return *report, budgetReached
}

func (r *Runner) failEntryStorageUpgrade(
	ctx context.Context,
	feed config.Feed,
	state *store.State,
	now time.Time,
	report *FeedReport,
	err error,
) bool {
	report.Status = "failed"
	report.Error = fmt.Sprintf("upgrade entry storage: %v", err)
	report.FailureClass = "storage"
	budgetReached := errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(ctx.Err(), context.DeadlineExceeded)
	if budgetReached {
		report.FailureClass = "budget"
	} else if errors.Is(err, context.Canceled) {
		report.FailureClass = "cancelled"
	}
	r.recordFailure(report, feed, state, now, err)
	return budgetReached
}

func (r *Runner) persistFetched(feed config.Feed, state *store.State, result fetch.Result, now time.Time) error {
	metadata := store.SnapshotMetadata{
		SchemaVersion: 1, FeedID: feed.ID, RequestedURL: feed.URL,
		EffectiveURL: result.EffectiveURL, Status: result.Status,
		ContentType: result.ContentType, ContentEncoding: result.ContentEncoding,
		ETag: result.ETag, LastModified: result.LastModified,
		RetrievedAt: result.RetrievedAt.UTC().Format(time.RFC3339),
		Redirects:   append([]string{}, result.Redirects...),
	}
	if err := r.Store.SaveSnapshot(feed.ID, result.Body, metadata); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	sum := hashHex(result.Body)
	state.PendingSnapshot = sum
	state.PendingETag = result.ETag
	state.PendingLastMod = result.LastModified
	state.PendingEffectiveURL = result.EffectiveURL
	state.PendingRetrievedAt = result.RetrievedAt.UTC().Format(time.RFC3339)
	state.PendingEncoding = result.ContentEncoding
	state.PendingContentType = result.ContentType
	state.PendingOffset = 0
	state.LastAttempt = now.UTC().Format(time.RFC3339)
	if err := r.Store.SaveState(feed.ID, *state); err != nil {
		return fmt.Errorf("mark snapshot pending: %w", err)
	}
	return nil
}

func (r *Runner) processPending(ctx context.Context, feed config.Feed, state store.State, report FeedReport, now time.Time) (FeedReport, bool) {
	report.Snapshot = state.PendingSnapshot
	report.EffectiveURL = state.PendingEffectiveURL
	body, err := r.Store.ReadSnapshot(feed.ID, state.PendingSnapshot)
	if err != nil {
		report.Status = "failed"
		report.FailureClass = "storage"
		report.Error = fmt.Sprintf("read pending snapshot: %v", err)
		r.recordFailure(&report, feed, &state, now, err)
		return report, false
	}
	decoded, err := fetch.DecodeBody(body, state.PendingEncoding, r.Config.HTTP.MaxBodyBytes)
	if err != nil {
		r.recordSnapshotFailure(&report, feed, &state, now, "content", err)
		return report, false
	}
	retrievedAt, err := time.Parse(time.RFC3339, state.PendingRetrievedAt)
	if err != nil || state.PendingRetrievedAt == "" {
		retrievedAt = now
	}
	parsed, err := parse.ParseRange(
		decoded, state.PendingEffectiveURL, feed.ID, state.PendingSnapshot,
		retrievedAt, state.PendingOffset, feed.MaxEntriesPerRun,
	)
	if err != nil {
		r.recordSnapshotFailure(&report, feed, &state, now, "parse", err)
		return report, false
	}
	report.Format = parsed.Format
	report.Warnings = append(report.Warnings, parsed.Warnings...)
	mediaType, _, _ := mime.ParseMediaType(state.PendingContentType)
	if strings.HasSuffix(mediaType, "html") || strings.Contains(mediaType, "text/html") {
		report.Warnings = append(report.Warnings, "successful feed parsed despite unexpected HTML Content-Type")
	}
	for _, entry := range parsed.Entries {
		if err := ctx.Err(); err != nil {
			report.Status = "failed"
			report.FailureClass = "budget"
			report.Error = err.Error()
			r.recordFailure(&report, feed, &state, now, err)
			return report, errors.Is(err, context.DeadlineExceeded)
		}
		newEntry, changed, err := r.Store.SaveEntry(entry, now)
		if err != nil {
			report.Status = "failed"
			report.FailureClass = "storage"
			report.Error = fmt.Sprintf("persist entry %s: %v", entry.EntryKey, err)
			r.recordFailure(&report, feed, &state, now, err)
			return report, false
		}
		report.Counts.EntriesSeen++
		if newEntry {
			report.Counts.NewEntries++
		} else if changed {
			report.Counts.Revisions++
		} else {
			report.Counts.Unchanged++
		}
	}

	state.PendingOffset += len(parsed.Entries)
	clearFailure(&state)
	report.RemainingEntries = parsed.RemainingEntries
	if parsed.RemainingEntries > 0 {
		report.Status = "partial"
		if err := r.Store.SaveState(feed.ID, state); err != nil {
			report.Status = "failed"
			report.FailureClass = "storage"
			report.Error = fmt.Sprintf("save pending progress: %v", err)
			return report, false
		}
		return report, true
	}

	state.ETag = state.PendingETag
	state.LastModified = state.PendingLastMod
	state.EffectiveURL = state.PendingEffectiveURL
	state.LastSuccess = now.UTC().Format(time.RFC3339)
	clearPending(&state)
	if err := r.Store.SaveState(feed.ID, state); err != nil {
		report.Status = "failed"
		report.FailureClass = "storage"
		report.Error = fmt.Sprintf("commit feed state: %v", err)
	}
	return report, false
}

func (r *Runner) recordSnapshotFailure(report *FeedReport, feed config.Feed, state *store.State, now time.Time, class string, err error) {
	report.Status = "failed"
	report.FailureClass = class
	report.Error = err.Error()
	// Keep the bytes, but fetch a replacement next time unless a batch already committed.
	if state.PendingOffset == 0 {
		clearPending(state)
	}
	r.recordFailure(report, feed, state, now, err)
}

func clearPending(state *store.State) {
	state.PendingSnapshot = ""
	state.PendingETag = ""
	state.PendingLastMod = ""
	state.PendingEffectiveURL = ""
	state.PendingRetrievedAt = ""
	state.PendingEncoding = ""
	state.PendingContentType = ""
	state.PendingOffset = 0
}

func due(state store.State, now time.Time, intervalSeconds int) bool {
	if next, err := time.Parse(time.RFC3339, state.NextRetryAt); err == nil {
		return !now.Before(next)
	}
	last, err := time.Parse(time.RFC3339, state.LastSuccess)
	return err != nil || now.Sub(last) >= time.Duration(intervalSeconds)*time.Second
}

func retryNotDue(state store.State, now time.Time) bool {
	next, err := time.Parse(time.RFC3339, state.NextRetryAt)
	return err == nil && now.Before(next)
}

func (r *Runner) recordFailure(report *FeedReport, feed config.Feed, state *store.State, now time.Time, err error) {
	state.ConsecutiveErrors++
	state.LastError = err.Error()
	state.FailureClass = report.FailureClass

	next := fetch.RetryAt(err)
	maxBackoff := time.Duration(r.Config.Retry.MaxBackoffSeconds) * time.Second
	if next.After(now.Add(maxBackoff)) {
		next = now.Add(maxBackoff)
	}
	if next.IsZero() || !next.After(now) {
		backoff := time.Duration(r.Config.Retry.InitialBackoffSeconds) * time.Second
		for attempt := 1; attempt < state.ConsecutiveErrors; attempt++ {
			if backoff >= time.Duration(r.Config.Retry.MaxBackoffSeconds)*time.Second/2 {
				backoff = time.Duration(r.Config.Retry.MaxBackoffSeconds) * time.Second
				break
			}
			backoff *= 2
		}
		percent := r.Config.Retry.JitterPercent
		if percent > 0 {
			span := int64(backoff) * int64(percent) / 100
			backoff += time.Duration(rand.Int64N(span*2+1) - span)
		}
		next = now.Add(backoff)
	}
	state.RetryExhausted = state.ConsecutiveErrors >= r.Config.Retry.MaxAttempts
	if state.RetryExhausted {
		next = now.Add(time.Duration(feed.PollIntervalSeconds) * time.Second)
	}
	state.NextRetryAt = next.UTC().Format(time.RFC3339)
	if saveErr := r.Store.SaveState(feed.ID, *state); saveErr != nil {
		report.Error += fmt.Sprintf("; save failure state: %v", saveErr)
	}
}

func clearFailure(state *store.State) {
	state.NextRetryAt = ""
	state.LastError = ""
	state.FailureClass = ""
	state.ConsecutiveErrors = 0
	state.RetryExhausted = false
}

func failureClass(err error) string {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "cancelled"
	case fetch.IsTransient(err):
		return "transport"
	default:
		return "http"
	}
}

func acquireLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("lock data directory: %w", err)
	}
	if err := file.Truncate(0); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("truncate lock: %w", err)
	}
	if _, err := file.WriteString(strconv.Itoa(os.Getpid())); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("write lock: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("sync lock: %w", err)
	}
	return file, nil
}

func releaseLock(file *os.File) {
	_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	_ = file.Close()
}

func hashHex(body []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(body))
}
