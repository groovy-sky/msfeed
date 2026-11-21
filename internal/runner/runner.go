package runner

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/groovy-sky/msfeed/internal/config"
	"github.com/groovy-sky/msfeed/internal/fetch"
	"github.com/groovy-sky/msfeed/internal/parse"
	"github.com/groovy-sky/msfeed/internal/store"
)

var ErrLocked = errors.New("another collector process holds the data directory lock")

type Report struct {
	SchemaVersion int          `json:"schema_version"`
	RunID         string       `json:"run_id"`
	StartedAt     string       `json:"started_at"`
	FinishedAt    string       `json:"finished_at"`
	Feeds         []FeedReport `json:"feeds"`
	Failed        int          `json:"failed"`
	BudgetReached bool         `json:"budget_reached,omitempty"`
}

type FeedReport struct {
	FeedID         string           `json:"feed_id"`
	Status         string           `json:"status"`
	Format         string           `json:"format,omitempty"`
	Snapshot       string           `json:"snapshot,omitempty"`
	EffectiveURL   string           `json:"effective_url,omitempty"`
	Counts         store.FeedCounts `json:"counts"`
	SkippedEntries int              `json:"skipped_entries,omitempty"`
	NotModified    bool             `json:"not_modified,omitempty"`
	Error          string           `json:"error,omitempty"`
}

type Runner struct {
	Config *config.Config
	Store  *store.Store
	Now    func() time.Time
}

func New(cfg *config.Config) *Runner {
	return &Runner{Config: cfg, Store: store.New(cfg.DataDir), Now: time.Now}
}

func (r *Runner) Run(ctx context.Context, onlyFeed string, force bool) (*Report, error) {
	if err := r.Store.Initialize(); err != nil {
		return nil, err
	}
	lock, err := acquireLock(filepath.Join(r.Store.Root(), "collector.lock"))
	if err != nil {
		return nil, err
	}
	defer releaseLock(lock)

	start := r.Now()
	runID := start.UTC().Format("20060102T150405.000000000Z")
	report := &Report{SchemaVersion: 1, RunID: runID, StartedAt: start.UTC().Format(time.RFC3339Nano)}
	budget, cancel := context.WithTimeout(ctx, r.Config.RunTimeout())
	defer cancel()

	selected := false
	for _, feed := range r.Config.Feeds {
		if !feed.Enabled || (onlyFeed != "" && feed.ID != onlyFeed) {
			continue
		}
		selected = true
		if err := r.Store.SaveFeed(store.FeedConfig{
			SchemaVersion: 1, ID: feed.ID, URL: feed.URL,
		}); err != nil {
			report.Feeds = append(report.Feeds, FeedReport{FeedID: feed.ID, Status: "failed", Error: err.Error()})
			report.Failed++
			continue
		}
		if err := budget.Err(); err != nil {
			report.BudgetReached = true
			break
		}
		feedReport := r.processFeed(budget, feed, force)
		report.Feeds = append(report.Feeds, feedReport)
		if feedReport.Status == "failed" {
			report.Failed++
		}
	}
	if onlyFeed != "" && !selected {
		return nil, fmt.Errorf("feed %q is not enabled or does not exist", onlyFeed)
	}
	report.FinishedAt = r.Now().UTC().Format(time.RFC3339Nano)
	if err := r.Store.SaveRun(runID, report); err != nil {
		return report, fmt.Errorf("save run report: %w", err)
	}
	return report, nil
}

func (r *Runner) processFeed(ctx context.Context, feed config.Feed, force bool) FeedReport {
	report := FeedReport{FeedID: feed.ID, Status: "completed"}
	state, err := r.Store.LoadState(feed.ID)
	if err != nil {
		report.Status = "failed"
		report.Error = fmt.Sprintf("load state: %v", err)
		return report
	}
	now := r.Now()
	state.LastAttempt = now.UTC().Format(time.RFC3339)
	if state.PendingSnapshot != "" {
		report = r.processPending(feed, state, report, now)
		return report
	}
	if !force && !due(state, now, feed.PollIntervalSeconds) {
		report.Status = "skipped"
		return report
	}

	result, err := fetch.One(ctx, fetch.Request{
		URL: feed.URL, ETag: state.ETag, LastModified: state.LastModified,
		UserAgent: r.Config.HTTP.UserAgent, Timeout: r.Config.HTTPTimeout(),
		MaxBodyBytes: r.Config.HTTP.MaxBodyBytes, MaxRedirects: r.Config.HTTP.MaxRedirects,
		AllowPrivateNetwork: r.Config.HTTP.AllowPrivateNetworks,
	})
	report.EffectiveURL = result.EffectiveURL
	if err != nil {
		report.Status = "failed"
		report.Error = err.Error()
		markFailure(&state, now, err)
		if saveErr := r.Store.SaveState(feed.ID, state); saveErr != nil {
			report.Error += fmt.Sprintf("; save failure state: %v", saveErr)
		}
		return report
	}
	if result.NotModified {
		report.NotModified = true
		state.LastSuccess = now.UTC().Format(time.RFC3339)
		state.NextRetryAt = ""
		state.LastError = ""
		state.ConsecutiveErrors = 0
		if err := r.Store.SaveState(feed.ID, state); err != nil {
			report.Status = "failed"
			report.Error = err.Error()
		}
		return report
	}

	metadata := store.SnapshotMetadata{
		SchemaVersion: 1, FeedID: feed.ID, RequestedURL: feed.URL,
		EffectiveURL: result.EffectiveURL, Status: result.Status,
		ContentType: result.ContentType, ETag: result.ETag,
		LastModified: result.LastModified, RetrievedAt: result.RetrievedAt.UTC().Format(time.RFC3339),
	}
	if err := r.Store.SaveSnapshot(feed.ID, result.Body, metadata); err != nil {
		report.Status = "failed"
		report.Error = fmt.Sprintf("save snapshot: %v", err)
		return report
	}
	sum := hashHex(result.Body)
	state.PendingSnapshot = sum
	state.PendingETag = result.ETag
	state.PendingLastMod = result.LastModified
	state.PendingEffectiveURL = result.EffectiveURL
	state.PendingRetrievedAt = result.RetrievedAt.UTC().Format(time.RFC3339)
	if err := r.Store.SaveState(feed.ID, state); err != nil {
		report.Status = "failed"
		report.Error = fmt.Sprintf("mark snapshot pending: %v", err)
		return report
	}
	report.Snapshot = sum
	return r.processPending(feed, state, report, now)
}

func (r *Runner) processPending(feed config.Feed, state store.State, report FeedReport, now time.Time) FeedReport {
	body, err := r.Store.ReadSnapshot(feed.ID, state.PendingSnapshot)
	if err != nil {
		report.Status = "failed"
		report.Error = fmt.Sprintf("read pending snapshot: %v", err)
		return report
	}
	retrievedAt, err := time.Parse(time.RFC3339, state.PendingRetrievedAt)
	if err != nil || state.PendingRetrievedAt == "" {
		retrievedAt = now
	}
	parsed, err := parse.Parse(body, state.PendingEffectiveURL, feed.ID, state.PendingSnapshot, retrievedAt, feed.MaxEntriesPerRun)
	if err != nil {
		report.Status = "failed"
		report.Error = err.Error()
		markFailure(&state, now, err)
		_ = r.Store.SaveState(feed.ID, state)
		return report
	}
	report.Format = parsed.Format
	report.Snapshot = state.PendingSnapshot
	report.EffectiveURL = state.PendingEffectiveURL
	report.SkippedEntries = parsed.EntryLimit
	for _, entry := range parsed.Entries {
		newEntry, changed, err := r.Store.SaveEntry(entry, now)
		if err != nil {
			report.Status = "failed"
			report.Error = fmt.Sprintf("persist entry %s: %v", entry.EntryKey, err)
			markFailure(&state, now, err)
			_ = r.Store.SaveState(feed.ID, state)
			return report
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
	state.ETag = state.PendingETag
	state.LastModified = state.PendingLastMod
	state.EffectiveURL = state.PendingEffectiveURL
	state.LastSuccess = now.UTC().Format(time.RFC3339)
	state.NextRetryAt = ""
	state.LastError = ""
	state.ConsecutiveErrors = 0
	state.PendingSnapshot = ""
	state.PendingETag = ""
	state.PendingLastMod = ""
	state.PendingEffectiveURL = ""
	state.PendingRetrievedAt = ""
	if err := r.Store.SaveState(feed.ID, state); err != nil {
		report.Status = "failed"
		report.Error = fmt.Sprintf("commit feed state: %v", err)
		return report
	}
	return report
}

func due(state store.State, now time.Time, intervalSeconds int) bool {
	if next, err := time.Parse(time.RFC3339, state.NextRetryAt); err == nil && now.Before(next) {
		return false
	}
	last, err := time.Parse(time.RFC3339, state.LastSuccess)
	return err != nil || now.Sub(last) >= time.Duration(intervalSeconds)*time.Second
}

func markFailure(state *store.State, now time.Time, err error) {
	state.ConsecutiveErrors++
	state.LastError = err.Error()
	backoff := 30 * time.Second
	for i := 1; i < state.ConsecutiveErrors && backoff < time.Hour; i++ {
		backoff *= 2
	}
	state.NextRetryAt = now.Add(backoff).UTC().Format(time.RFC3339)
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
	_, _ = file.WriteString(strconv.Itoa(os.Getpid()))
	_ = file.Sync()
	return file, nil
}

func releaseLock(file *os.File) {
	_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	_ = file.Close()
}

func hashHex(body []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(body))
}
