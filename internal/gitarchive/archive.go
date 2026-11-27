package gitarchive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/groovy-sky/msfeed/internal/classify"
	"github.com/groovy-sky/msfeed/internal/config"
	"github.com/groovy-sky/msfeed/internal/runner"
)

const (
	defaultBranchPrefix = "rss/"
	authorName          = "rsscollector archive"
	authorEmail         = "rsscollector@users.noreply.github.com"
	branchIgnore        = "collector.lock\nruns/\n"
)

var ErrConfiguration = errors.New("invalid git archive configuration")

type Options struct {
	Config       *config.Config
	RepoDir      string
	Remote       string
	BranchPrefix string
	OnlyFeed     string
	Force        bool
	DryRun       bool
	Classifier   classify.Classifier
	Now          func() time.Time
}

type Report struct {
	SchemaVersion int          `json:"schema_version"`
	StartedAt     string       `json:"started_at"`
	FinishedAt    string       `json:"finished_at"`
	Remote        string       `json:"remote"`
	DryRun        bool         `json:"dry_run"`
	Feeds         []FeedReport `json:"feeds"`
	Requests      int          `json:"requests"`
	Changed       int          `json:"changed"`
	Pushed        int          `json:"pushed"`
	Failed        int          `json:"failed"`
	BudgetReached bool         `json:"budget_reached,omitempty"`
}

type FeedReport struct {
	FeedID                   string         `json:"feed_id"`
	Branch                   string         `json:"branch"`
	BaseCommit               string         `json:"base_commit,omitempty"`
	Commit                   string         `json:"commit,omitempty"`
	Status                   string         `json:"status"`
	Changed                  bool           `json:"changed,omitempty"`
	Collector                *runner.Report `json:"collector,omitempty"`
	Error                    string         `json:"error,omitempty"`
	Requests                 int            `json:"requests"`
	EntriesSeen              int            `json:"entries_seen"`
	NewEntries               int            `json:"new_entries"`
	Revisions                int            `json:"revisions"`
	Unchanged                int            `json:"unchanged"`
	Enrichments              int            `json:"enrichments,omitempty"`
	EnrichmentRevisions      int            `json:"enrichment_revisions,omitempty"`
	EnrichmentDeferred       int            `json:"enrichment_deferred,omitempty"`
	Classifications          int            `json:"classifications,omitempty"`
	ClassificationRevisions  int            `json:"classification_revisions,omitempty"`
	RemainingClassifications int            `json:"remaining_classifications,omitempty"`
	FailureClass             string         `json:"failure_class,omitempty"`
}

type feedTarget struct {
	feed      config.Feed
	branch    string
	base      string
	baseRef   string
	commitRef string
}

type gitCommand struct {
	repo string
}

func Publish(ctx context.Context, options Options) (*Report, error) {
	if options.Config == nil {
		return nil, fmt.Errorf("%w: config is required", ErrConfiguration)
	}
	repo, err := findRepo(ctx, options.RepoDir)
	if err != nil {
		return nil, err
	}
	if options.Remote == "" {
		options.Remote = "origin"
	}
	if options.BranchPrefix == "" {
		options.BranchPrefix = defaultBranchPrefix
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if strings.HasPrefix(options.Remote, "-") {
		return nil, fmt.Errorf("%w: remote must not begin with '-'", ErrConfiguration)
	}

	targets, feedBudgetReached, err := selectTargets(ctx, repo, options)
	if err != nil {
		return nil, err
	}
	now := options.Now()
	report := &Report{
		SchemaVersion: 1,
		StartedAt:     now.UTC().Format(time.RFC3339Nano),
		Remote:        options.Remote,
		DryRun:        options.DryRun,
		Feeds:         make([]FeedReport, 0, len(targets)),
		BudgetReached: feedBudgetReached,
	}
	timeout := options.Config.RunTimeout()
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	runCtx, cancelRun := context.WithTimeout(ctx, timeout)
	defer cancelRun()
	namespace := fmt.Sprintf(
		"refs/rsscollector/archive/%d-%d",
		time.Now().UnixNano(), os.Getpid(),
	)
	for index := range targets {
		targets[index].baseRef = namespace + "/base/" + targets[index].branch
		targets[index].commitRef = namespace + "/commit/" + targets[index].branch
	}
	cleanupRefs := func() {
		for _, target := range targets {
			_, _ = gitCommand{repo: repo}.run(context.Background(), "update-ref", "-d", target.baseRef)
			_, _ = gitCommand{repo: repo}.run(context.Background(), "update-ref", "-d", target.commitRef)
		}
	}
	defer cleanupRefs()

	remoteHeads, err := listRemoteHeads(runCtx, repo, options.Remote, targets)
	if err != nil {
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			report.BudgetReached = true
			return finishReport(report, options.Now), nil
		}
		return finishReport(report, options.Now), err
	}
	fetchedHeads, err := fetchHeads(runCtx, repo, options.Remote, targets, remoteHeads)
	if err != nil {
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			report.BudgetReached = true
			return finishReport(report, options.Now), nil
		}
		return finishReport(report, options.Now), err
	}

	pushArgs := make([]string, 0, len(targets))
	pushRefspecs := make([]string, 0, len(targets))
	requestBudget := options.Config.HTTP.MaxRequestsPerRun
	if requestBudget <= 0 {
		requestBudget = 100
	}
	for _, target := range targets {
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			report.BudgetReached = true
			break
		}
		if runCtx.Err() != nil {
			break
		}
		feedReport := FeedReport{
			FeedID: target.feed.ID,
			Branch: target.branch,
			Status: "failed",
		}
		base, ok := fetchedHeads[target.branch]
		if ok {
			target.base = base
			feedReport.BaseCommit = base
		}
		processed, err := processTarget(runCtx, ctx, repo, options, target, requestBudget)
		feedReport = mergeFeedReports(feedReport, processed)
		mergeCollectorMetrics(&feedReport, processed.Collector)
		report.Requests += feedReport.Requests
		requestBudget -= feedReport.Requests
		if feedReport.Collector != nil && feedReport.Collector.BudgetReached {
			report.BudgetReached = true
		}
		if err != nil {
			feedReport.FeedID = target.feed.ID
			feedReport.Branch = target.branch
			feedReport.Error = err.Error()
			report.Failed++
			report.Feeds = append(report.Feeds, feedReport)
			if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
				report.BudgetReached = true
				break
			}
			continue
		}
		report.Changed += boolInt(feedReport.Changed)
		if feedReport.Collector != nil && feedReport.Collector.Failed > 0 {
			report.Failed += feedReport.Collector.Failed
		}
		report.Feeds = append(report.Feeds, feedReport)
		if feedReport.Changed && !options.DryRun {
			pushArgs = append(pushArgs, "--force-with-lease=refs/heads/"+target.branch+":"+target.base)
			pushRefspecs = append(
				pushRefspecs,
				target.commitRef+":refs/heads/"+target.branch,
			)
		}
	}

	if len(pushRefspecs) > 0 && ctx.Err() == nil {
		args := []string{"push", "--atomic"}
		args = append(args, pushArgs...)
		args = append(args, options.Remote)
		args = append(args, pushRefspecs...)
		if _, err := (gitCommand{repo: repo}).run(ctx, args...); err != nil {
			return finishReport(report, options.Now), fmt.Errorf("publish feed branches: %w", err)
		}
		report.Pushed = len(pushRefspecs)
	}
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		report.BudgetReached = true
	}
	if err := ctx.Err(); err != nil {
		return finishReport(report, options.Now), err
	}
	return finishReport(report, options.Now), nil
}

func finishReport(report *Report, now func() time.Time) *Report {
	report.FinishedAt = now().UTC().Format(time.RFC3339Nano)
	sort.Slice(report.Feeds, func(left, right int) bool {
		return report.Feeds[left].FeedID < report.Feeds[right].FeedID
	})
	return report
}

func mergeFeedReports(base, incoming FeedReport) FeedReport {
	incoming.FeedID = base.FeedID
	incoming.Branch = base.Branch
	if incoming.BaseCommit == "" {
		incoming.BaseCommit = base.BaseCommit
	}
	if incoming.Status == "" {
		incoming.Status = base.Status
	}
	return incoming
}

func mergeCollectorMetrics(report *FeedReport, collector *runner.Report) {
	if collector == nil {
		return
	}
	report.Collector = collector
	report.Requests = collector.Requests
	if len(collector.Feeds) != 1 {
		return
	}
	feed := collector.Feeds[0]
	report.EntriesSeen = feed.Counts.EntriesSeen
	report.NewEntries = feed.Counts.NewEntries
	report.Revisions = feed.Counts.Revisions
	report.Unchanged = feed.Counts.Unchanged
	report.Enrichments = feed.Counts.Enrichments
	report.EnrichmentRevisions = feed.Counts.EnrichmentRevisions
	report.EnrichmentDeferred = feed.Counts.EnrichmentDeferred
	report.Classifications = feed.Counts.Classifications
	report.ClassificationRevisions = feed.Counts.ClassificationRevisions
	report.RemainingClassifications = feed.RemainingClassifications
	report.FailureClass = feed.FailureClass
}

func findRepo(ctx context.Context, path string) (string, error) {
	if path == "" {
		path = "."
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("%w: resolve repository: %v", ErrConfiguration, err)
	}
	output, err := (gitCommand{repo: absolute}).run(ctx, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrConfiguration, err)
	}
	return strings.TrimSpace(string(output)), nil
}

func selectTargets(ctx context.Context, repo string, options Options) ([]feedTarget, bool, error) {
	seenURLs := make(map[string]string, len(options.Config.Feeds))
	targets := make([]feedTarget, 0, len(options.Config.Feeds))
	selected := false
	probe := "refs/heads/" + branchName(options.BranchPrefix, "archive-probe")
	if _, err := (gitCommand{repo: repo}).run(ctx, "check-ref-format", probe); err != nil {
		return nil, false, fmt.Errorf(
			"%w: invalid branch prefix %q: %v",
			ErrConfiguration, options.BranchPrefix, err,
		)
	}
	for _, feed := range options.Config.Feeds {
		if !feed.Enabled || (options.OnlyFeed != "" && feed.ID != options.OnlyFeed) {
			continue
		}
		selected = true
		if strings.Contains(feed.ID, "..") ||
			strings.HasSuffix(feed.ID, ".") ||
			strings.HasSuffix(strings.ToLower(feed.ID), ".lock") {
			return nil, false, fmt.Errorf(
				"%w: feed id %q cannot be used as a Git branch component",
				ErrConfiguration, feed.ID,
			)
		}
		branch := branchName(options.BranchPrefix, feed.ID)
		source := normalizedSourceURL(feed.URL)
		if previous, ok := seenURLs[source]; ok {
			return nil, false, fmt.Errorf(
				"%w: feeds %q and %q refer to the same URL %q",
				ErrConfiguration, previous, feed.ID, feed.URL,
			)
		}
		seenURLs[source] = feed.ID
		targets = append(targets, feedTarget{feed: feed, branch: branch})
		if options.Config.MaxFeedsPerRun > 0 &&
			len(targets) >= options.Config.MaxFeedsPerRun {
			limited := false
			for _, remaining := range options.Config.Feeds {
				if remaining.Enabled &&
					(options.OnlyFeed == "" || remaining.ID == options.OnlyFeed) &&
					remaining.ID != feed.ID &&
					!targetExists(targets, remaining.ID) {
					limited = true
					break
				}
			}
			return targets, limited, nil
		}
	}
	if options.OnlyFeed != "" && !selected {
		return nil, false, fmt.Errorf(
			"%w: feed %q is not enabled or does not exist",
			ErrConfiguration, options.OnlyFeed,
		)
	}
	return targets, false, nil
}

func targetExists(targets []feedTarget, feedID string) bool {
	for _, target := range targets {
		if target.feed.ID == feedID {
			return true
		}
	}
	return false
}

func branchName(prefix, feedID string) string {
	return strings.TrimSuffix(prefix, "/") + "/" + feedID
}

func normalizedSourceURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.ToLower(parsed.Host)
	parsed.Fragment = ""
	return parsed.String()
}

func listRemoteHeads(ctx context.Context, repo, remote string, targets []feedTarget) (map[string]string, error) {
	if len(targets) == 0 {
		return map[string]string{}, nil
	}
	args := []string{"ls-remote", "--heads", remote}
	for _, target := range targets {
		args = append(args, "refs/heads/"+target.branch)
	}
	output, err := (gitCommand{repo: repo}).run(ctx, args...)
	if err != nil {
		return nil, fmt.Errorf("list archive branches: %w", err)
	}
	result := make(map[string]string, len(targets))
	for _, line := range bytes.Split(output, []byte{'\n'}) {
		fields := bytes.Fields(line)
		if len(fields) != 2 {
			continue
		}
		ref := string(fields[1])
		branch := strings.TrimPrefix(ref, "refs/heads/")
		result[branch] = string(fields[0])
	}
	return result, nil
}

func fetchHeads(
	ctx context.Context,
	repo, remote string,
	targets []feedTarget,
	remoteHeads map[string]string,
) (map[string]string, error) {
	refspecs := make([]string, 0, len(targets))
	existing := make([]feedTarget, 0, len(targets))
	for _, target := range targets {
		if _, ok := remoteHeads[target.branch]; !ok {
			continue
		}
		refspecs = append(
			refspecs,
			"+refs/heads/"+target.branch+":"+target.baseRef,
		)
		existing = append(existing, target)
	}
	if len(refspecs) == 0 {
		return map[string]string{}, nil
	}
	args := []string{"fetch", "--quiet", "--no-tags", "--depth=1", remote}
	args = append(args, refspecs...)
	if _, err := (gitCommand{repo: repo}).run(ctx, args...); err != nil {
		return nil, fmt.Errorf("fetch archive branches: %w", err)
	}
	refs := make([]string, 0, len(existing))
	for _, target := range existing {
		refs = append(refs, target.baseRef)
	}
	output, err := (gitCommand{repo: repo}).run(ctx, append([]string{"rev-parse"}, refs...)...)
	if err != nil {
		return nil, fmt.Errorf("resolve fetched archive branches: %w", err)
	}
	commits := strings.Fields(string(output))
	if len(commits) != len(existing) {
		return nil, fmt.Errorf(
			"resolve fetched archive branches: got %d commits for %d refs",
			len(commits), len(existing),
		)
	}
	result := make(map[string]string, len(existing))
	for index, target := range existing {
		result[target.branch] = commits[index]
	}
	return result, nil
}

func processTarget(
	collectionCtx context.Context,
	finalizeCtx context.Context,
	repo string,
	options Options,
	target feedTarget,
	requestBudget int,
) (FeedReport, error) {
	tempDir, err := os.MkdirTemp("", "rsscollector-archive-*")
	if err != nil {
		return FeedReport{}, fmt.Errorf("create archive work directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	workDir := filepath.Join(tempDir, "data")
	indexPath := filepath.Join(tempDir, "index")
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		return FeedReport{}, fmt.Errorf("create archive data directory: %w", err)
	}
	git := gitCommand{
		repo: repo,
	}
	extraEnv := []string{
		"GIT_INDEX_FILE=" + indexPath,
		"GIT_WORK_TREE=" + workDir,
	}
	if target.base != "" {
		if err := validateBranchTree(collectionCtx, repo, target.base, target.feed.ID); err != nil {
			return FeedReport{}, err
		}
		if _, err := git.runEnv(collectionCtx, extraEnv, "read-tree", target.base); err != nil {
			return FeedReport{}, fmt.Errorf("read branch index: %w", err)
		}
		if _, err := git.runEnv(collectionCtx, extraEnv, "checkout-index", "--all", "--force", "--prefix="+workDir+string(filepath.Separator)); err != nil {
			return FeedReport{}, fmt.Errorf("materialize archive branch: %w", err)
		}
	} else if _, err := git.runEnv(collectionCtx, extraEnv, "read-tree", "--empty"); err != nil {
		return FeedReport{}, fmt.Errorf("initialize branch index: %w", err)
	}

	if err := os.WriteFile(filepath.Join(workDir, ".gitignore"), []byte(branchIgnore), 0o600); err != nil {
		return FeedReport{}, fmt.Errorf("write branch ignore file: %w", err)
	}
	runConfig := *options.Config
	runConfig.DataDir = workDir
	runConfig.Feeds = []config.Feed{target.feed}
	runConfig.HTTP.MaxRequestsPerRun = requestBudget
	collector := runner.New(&runConfig)
	collector.Config.HTTP.MaxRequestsPerRun = requestBudget
	collector.Classifier = options.Classifier
	collector.Now = options.Now
	runReport, runErr := collector.Run(collectionCtx, target.feed.ID, options.Force)
	report := FeedReport{
		FeedID:     target.feed.ID,
		Branch:     target.branch,
		BaseCommit: target.base,
		Status:     "failed",
		Collector:  runReport,
	}
	if runErr != nil {
		return report, fmt.Errorf("collect %s: %w", target.feed.ID, runErr)
	}
	if runReport == nil || len(runReport.Feeds) != 1 {
		return report, errors.New("collector returned an invalid feed report")
	}
	report.Status = runReport.Feeds[0].Status
	if options.Classifier != nil && runReport.Failed > 0 {
		return report, fmt.Errorf(
			"collect %s with classification failed: %s",
			target.feed.ID, runReport.Feeds[0].Error,
		)
	}

	if _, err := git.runEnv(finalizeCtx, extraEnv, "add", "--all", "--", ".gitignore", "feeds"); err != nil {
		return report, fmt.Errorf("stage archive data: %w", err)
	}
	treeOutput, err := git.runEnv(finalizeCtx, extraEnv, "write-tree")
	if err != nil {
		return report, fmt.Errorf("write archive tree: %w", err)
	}
	tree := strings.TrimSpace(string(treeOutput))
	if target.base != "" {
		baseTree, err := git.run(finalizeCtx, "rev-parse", target.base+"^{tree}")
		if err != nil {
			return report, fmt.Errorf("read base archive tree: %w", err)
		}
		if tree == strings.TrimSpace(string(baseTree)) {
			return report, nil
		}
	}

	message := fmt.Sprintf("Archive %s", target.feed.ID)
	commitArgs := []string{"commit-tree", tree, "-m", message}
	commitEnv := append(extraEnv,
		"GIT_AUTHOR_NAME="+authorName,
		"GIT_AUTHOR_EMAIL="+authorEmail,
		"GIT_COMMITTER_NAME="+authorName,
		"GIT_COMMITTER_EMAIL="+authorEmail,
		"GIT_AUTHOR_DATE="+options.Now().Format(time.RFC3339),
		"GIT_COMMITTER_DATE="+options.Now().Format(time.RFC3339),
	)
	if target.base != "" {
		commitArgs = append(commitArgs, "-p", target.base)
	}
	commitOutput, err := git.runEnv(finalizeCtx, commitEnv, commitArgs...)
	if err != nil {
		return report, fmt.Errorf("create archive commit: %w", err)
	}
	commit := strings.TrimSpace(string(commitOutput))
	if _, err := git.run(finalizeCtx, "update-ref", target.commitRef, commit); err != nil {
		return report, fmt.Errorf("record archive commit: %w", err)
	}
	report.Changed = true
	report.Commit = commit
	return report, nil
}

func validateBranchTree(ctx context.Context, repo, commit, feedID string) error {
	output, err := (gitCommand{repo: repo}).run(ctx, "ls-tree", "-r", "-z", commit)
	if err != nil {
		return fmt.Errorf("inspect archive branch: %w", err)
	}
	allowedPrefix := "feeds/" + feedID + "/"
	for _, entry := range bytes.Split(output, []byte{0}) {
		if len(entry) == 0 {
			continue
		}
		parts := bytes.SplitN(entry, []byte{'\t'}, 2)
		if len(parts) != 2 {
			return errors.New("inspect archive branch: invalid tree record")
		}
		header := strings.Fields(string(parts[0]))
		if len(header) != 3 {
			return errors.New("inspect archive branch: invalid tree header")
		}
		mode := header[0]
		objectType := header[1]
		path := string(parts[1])
		if mode != "100644" && mode != "100755" {
			return fmt.Errorf("archive branch contains non-regular object %q", path)
		}
		if objectType != "blob" {
			return fmt.Errorf("archive branch contains unsupported object %q", path)
		}
		if path != ".gitignore" && !strings.HasPrefix(path, allowedPrefix) {
			return fmt.Errorf("archive branch contains unrelated path %q", path)
		}
	}
	return nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (g gitCommand) run(ctx context.Context, args ...string) ([]byte, error) {
	return g.runEnv(ctx, nil, args...)
}

func (g gitCommand) runEnv(ctx context.Context, env []string, args ...string) ([]byte, error) {
	commandArgs := append([]string{"-C", g.repo}, args...)
	command := exec.CommandContext(ctx, "git", commandArgs...)
	command.Env = append(os.Environ(), env...)
	output, err := command.CombinedOutput()
	if err == nil {
		return output, nil
	}
	message := strings.TrimSpace(string(output))
	if message == "" {
		message = err.Error()
	}
	return nil, fmt.Errorf("git %s: %s", strings.Join(args, " "), message)
}
