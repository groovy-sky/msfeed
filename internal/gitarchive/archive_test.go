package gitarchive

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groovy-sky/msfeed/internal/config"
)

func TestPublishCreatesIsolatedBranchAndSkipsDueState(t *testing.T) {
	var requests atomic.Int32
	etag := `"v1"`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/rss+xml")
		if req.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = fmt.Fprint(w, `<?xml version="1.0"?>
			<rss version="2.0"><channel><title>Example</title>
			<item><guid>stable-id</guid><title>Hello</title><link>/hello</link></item>
			</channel></rss>`)
	}))
	defer server.Close()

	repo, remote := initGitRepository(t)
	cfg := testConfig(t, []config.Feed{{
		ID: "example", URL: server.URL, Enabled: true,
	}})
	now := time.Date(2026, 11, 21, 20, 0, 0, 0, time.UTC)

	first, err := Publish(context.Background(), Options{
		Config: cfg, RepoDir: repo, Remote: remote, Force: true,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Failed != 0 || first.Changed != 1 || first.Pushed != 1 ||
		first.Requests != 1 || first.Feeds[0].NewEntries != 1 {
		t.Fatalf("first report = %+v", first)
	}
	if got := first.Feeds[0].Branch; got != "rss/example" {
		t.Fatalf("branch = %q, want rss/example", got)
	}
	paths := gitLines(t, "", "--git-dir", remote, "ls-tree", "-r", "--name-only", "refs/heads/rss/example")
	for _, path := range paths {
		if path != ".gitignore" && !strings.HasPrefix(path, "feeds/example/") {
			t.Fatalf("branch contains unrelated path %q", path)
		}
	}
	if !containsPath(paths, "feeds/example/state.json") ||
		!containsPath(paths, "feeds/example/feed.json") {
		t.Fatalf("branch paths = %v", paths)
	}
	if anyPathPrefix(paths, "runs/") {
		t.Fatalf("run reports were archived: %v", paths)
	}

	secondNow := now.Add(time.Minute)
	second, err := Publish(context.Background(), Options{
		Config: cfg, RepoDir: repo, Remote: remote,
		Now: func() time.Time { return secondNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Failed != 0 || second.Changed != 0 || second.Pushed != 0 {
		t.Fatalf("second report = %+v", second)
	}
	if second.Feeds[0].Status != "skipped" {
		t.Fatalf("second status = %q, want skipped", second.Feeds[0].Status)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
}

func TestPublishBatchesMultipleFeedBranches(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = fmt.Fprintf(
			w,
			`<?xml version="1.0"?><rss version="2.0"><channel><title>%s</title>`+
				`<item><guid>%s</guid><title>Item</title></item></channel></rss>`,
			req.URL.Path, req.URL.Path,
		)
	}))
	defer server.Close()

	repo, remote := initGitRepository(t)
	cfg := testConfig(t, []config.Feed{
		{ID: "alpha", URL: server.URL + "/alpha", Enabled: true},
		{ID: "beta", URL: server.URL + "/beta", Enabled: true},
	})
	report, err := Publish(context.Background(), Options{
		Config: cfg, RepoDir: repo, Remote: remote, Force: true,
		Now: func() time.Time {
			return time.Date(2026, 11, 21, 21, 0, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed != 0 || report.Changed != 2 || report.Pushed != 2 {
		t.Fatalf("report = %+v", report)
	}
	alphaPaths := gitLines(t, "", "--git-dir", remote, "ls-tree", "-r", "--name-only", "refs/heads/rss/alpha")
	betaPaths := gitLines(t, "", "--git-dir", remote, "ls-tree", "-r", "--name-only", "refs/heads/rss/beta")
	if anyPathPrefix(alphaPaths, "feeds/beta/") || anyPathPrefix(betaPaths, "feeds/alpha/") {
		t.Fatalf("feed branches overlap: alpha=%v beta=%v", alphaPaths, betaPaths)
	}
}

func TestPublishDryRunDoesNotCreateRemoteBranch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">
			<title>Dry run</title><id>urn:feed</id><entry><id>urn:entry</id><title>One</title>
			<updated>2026-11-21T00:00:00Z</updated></entry></feed>`)
	}))
	defer server.Close()

	repo, remote := initGitRepository(t)
	cfg := testConfig(t, []config.Feed{{
		ID: "dry", URL: server.URL, Enabled: true,
	}})
	report, err := Publish(context.Background(), Options{
		Config: cfg, RepoDir: repo, Remote: remote, Force: true, DryRun: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.DryRun || report.Changed != 1 || report.Pushed != 0 {
		t.Fatalf("report = %+v", report)
	}
	output, err := exec.Command("git", "--git-dir", remote, "show-ref", "--verify", "--quiet", "refs/heads/rss/dry").CombinedOutput()
	if err == nil {
		t.Fatalf("dry-run branch exists unexpectedly: %s", output)
	}
}

func TestPublishHonorsFeedAndRequestBudgets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = fmt.Fprintf(
			w,
			`<?xml version="1.0"?><rss version="2.0"><channel><title>%s</title></channel></rss>`,
			req.URL.Path,
		)
	}))
	defer server.Close()

	t.Run("feed budget", func(t *testing.T) {
		repo, remote := initGitRepository(t)
		cfg := testConfig(t, []config.Feed{
			{ID: "alpha", URL: server.URL + "/alpha", Enabled: true},
			{ID: "beta", URL: server.URL + "/beta", Enabled: true},
		})
		cfg.MaxFeedsPerRun = 1
		report, err := Publish(context.Background(), Options{
			Config: cfg, RepoDir: repo, Remote: remote, Force: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Feeds) != 1 || !report.BudgetReached || report.Pushed != 1 {
			t.Fatalf("report = %+v", report)
		}
	})

	t.Run("request budget", func(t *testing.T) {
		repo, remote := initGitRepository(t)
		cfg := testConfig(t, []config.Feed{
			{ID: "alpha", URL: server.URL + "/alpha", Enabled: true},
			{ID: "beta", URL: server.URL + "/beta", Enabled: true},
		})
		cfg.HTTP.MaxRequestsPerRun = 1
		report, err := Publish(context.Background(), Options{
			Config: cfg, RepoDir: repo, Remote: remote, Force: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Feeds) != 2 || report.Requests != 1 ||
			!report.BudgetReached || report.Pushed != 2 {
			t.Fatalf("report = %+v", report)
		}
		if report.Feeds[1].Status != "deferred" {
			t.Fatalf("second status = %q, want deferred", report.Feeds[1].Status)
		}
	})
}

func TestPublishRejectsBranchWithUnrelatedPaths(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<?xml version="1.0"?><rss version="2.0"><channel><title>One</title></channel></rss>`)
	}))
	defer server.Close()

	repo, remote := initGitRepository(t)
	cfg := testConfig(t, []config.Feed{{
		ID: "example", URL: server.URL, Enabled: true,
	}})
	_, err := Publish(context.Background(), Options{
		Config: cfg, RepoDir: repo, Remote: remote, Force: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	runGit(t, repo, "fetch", "--quiet", remote, "refs/heads/rss/example")
	runGit(t, repo, "checkout", "--quiet", "-B", "rss/example", "FETCH_HEAD")
	if err := os.WriteFile(filepath.Join(repo, "unrelated.txt"), []byte("unrelated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repo, "add", "unrelated.txt")
	runGit(t, repo, "-c", "user.name=Test", "-c", "user.email=test@example.com",
		"commit", "--quiet", "-m", "Add unrelated file")
	runGit(t, repo, "push", "--quiet", remote, "HEAD:refs/heads/rss/example")
	before := strings.TrimSpace(string(runGit(t, repo, "rev-parse", "HEAD")))

	report, err := Publish(context.Background(), Options{
		Config: cfg, RepoDir: repo, Remote: remote, Force: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	after := strings.TrimSpace(string(runGit(
		t, "", "--git-dir", remote, "rev-parse", "refs/heads/rss/example",
	)))
	if report.Failed != 1 || report.Pushed != 0 ||
		!strings.Contains(report.Feeds[0].Error, "unrelated path") {
		t.Fatalf("report = %+v", report)
	}
	if after != before {
		t.Fatalf("remote advanced from %s to %s", before, after)
	}
}

func TestPublishRejectsDuplicateURLsAndUnknownFeed(t *testing.T) {
	repo, remote := initGitRepository(t)
	cfg := testConfig(t, []config.Feed{
		{ID: "one", URL: "https://EXAMPLE.com/feed", Enabled: true},
		{ID: "two", URL: "https://example.com/feed", Enabled: true},
	})
	_, err := Publish(context.Background(), Options{
		Config: cfg, RepoDir: repo, Remote: remote,
	})
	if !errors.Is(err, ErrConfiguration) {
		t.Fatalf("duplicate error = %v, want configuration error", err)
	}
	_, err = Publish(context.Background(), Options{
		Config: cfg, RepoDir: repo, Remote: remote, OnlyFeed: "missing",
	})
	if !errors.Is(err, ErrConfiguration) {
		t.Fatalf("unknown feed error = %v, want configuration error", err)
	}
	cfg.Feeds = []config.Feed{{
		ID: "invalid..feed", URL: "https://example.com/other", Enabled: true,
	}}
	_, err = Publish(context.Background(), Options{
		Config: cfg, RepoDir: repo, Remote: remote,
	})
	if !errors.Is(err, ErrConfiguration) {
		t.Fatalf("invalid branch id error = %v, want configuration error", err)
	}
}

func testConfig(t *testing.T, feeds []config.Feed) *config.Config {
	t.Helper()
	cfg := &config.Config{
		SchemaVersion:  1,
		DataDir:        "unused",
		MaxRunSeconds:  30,
		MaxFeedsPerRun: len(feeds),
		HTTP: config.HTTP{
			TimeoutSeconds:       5,
			MaxBodyBytes:         1 << 20,
			MaxRedirects:         3,
			MaxRequestsPerRun:    20,
			AllowPrivateNetworks: true,
		},
		Defaults: config.Defaults{
			PollIntervalSeconds: 3600,
			MaxEntriesPerRun:    100,
		},
		Feeds: feeds,
	}
	for index := range cfg.Feeds {
		if cfg.Feeds[index].PollIntervalSeconds == 0 {
			cfg.Feeds[index].PollIntervalSeconds = 3600
		}
		if cfg.Feeds[index].MaxEntriesPerRun == 0 {
			cfg.Feeds[index].MaxEntriesPerRun = 100
		}
	}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func initGitRepository(t *testing.T) (repo, remote string) {
	t.Helper()
	root := t.TempDir()
	remote = filepath.Join(root, "remote.git")
	repo = filepath.Join(root, "repo")
	seed := filepath.Join(root, "seed")
	runGit(t, "", "init", "--quiet", "--bare", remote)
	runGit(t, "", "init", "--quiet", seed)
	runGit(t, seed, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--quiet", "--allow-empty", "-m", "Initial")
	runGit(t, seed, "branch", "-M", "main")
	runGit(t, seed, "remote", "add", "origin", remote)
	runGit(t, seed, "push", "--quiet", "origin", "main")
	runGit(t, "", "--git-dir", remote, "symbolic-ref", "HEAD", "refs/heads/main")
	runGit(t, "", "clone", "--quiet", "--depth=1", "file://"+remote, repo)
	if got := strings.TrimSpace(string(runGit(t, repo, "rev-parse", "--is-shallow-repository"))); got != "true" {
		t.Fatalf("test repository is not shallow: %q", got)
	}
	return repo, remote
}

func runGit(t *testing.T, repo string, args ...string) []byte {
	t.Helper()
	return gitOutput(t, repo, args...)
}

func gitOutput(t *testing.T, repo string, args ...string) []byte {
	t.Helper()
	if repo != "" {
		args = append([]string{"-C", repo}, args...)
	}
	command := exec.Command("git", args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return output
}

func gitLines(t *testing.T, repo string, args ...string) []string {
	t.Helper()
	output := strings.TrimSpace(string(gitOutput(t, repo, args...)))
	if output == "" {
		return nil
	}
	return strings.Split(output, "\n")
}

func containsPath(paths []string, wanted string) bool {
	for _, path := range paths {
		if path == wanted {
			return true
		}
	}
	return false
}

func anyPathPrefix(paths []string, prefix string) bool {
	for _, path := range paths {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}
