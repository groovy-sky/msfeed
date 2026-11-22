package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/groovy-sky/msfeed/internal/config"
	"github.com/groovy-sky/msfeed/internal/gitarchive"
	"github.com/groovy-sky/msfeed/internal/runner"
	"github.com/groovy-sky/msfeed/internal/store"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		usage()
		return 2
	}
	switch args[0] {
	case "validate":
		return validate(args[1:])
	case "run":
		return collect(args[1:])
	case "status":
		return status(args[1:])
	case "archive":
		return archive(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", args[0])
		usage()
		return 2
	}
}

func validate(args []string) int {
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	configPath := flags.String("config", "config.json", "path to JSON configuration")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return 2
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		return 2
	}
	fmt.Printf("configuration is valid: %d feed(s), data directory %s\n", len(cfg.Feeds), cfg.DataDir)
	return 0
}

func collect(args []string) int {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	configPath := flags.String("config", "config.json", "path to JSON configuration")
	onlyFeed := flags.String("feed", "", "collect one feed ID")
	force := flags.Bool("force", false, "bypass poll and retry schedules")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return 2
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	report, err := runner.New(cfg).Run(ctx, *onlyFeed, *force)
	if err != nil {
		if errors.Is(err, runner.ErrLocked) {
			fmt.Fprintln(os.Stderr, err)
			return 3
		}
		if errors.Is(err, runner.ErrStorage) {
			fmt.Fprintf(os.Stderr, "collector failed: %v\n", err)
			return 1
		}
		fmt.Fprintf(os.Stderr, "collector failed: %v\n", err)
		return 2
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintf(os.Stderr, "write report: %v\n", err)
		return 1
	}
	if report.BudgetReached {
		return 4
	}
	if report.Failed > 0 {
		return 1
	}
	return 0
}

func status(args []string) int {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	configPath := flags.String("config", "config.json", "path to JSON configuration")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return 2
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		return 2
	}
	st := store.New(cfg.DataDir)
	feeds := make([]any, 0, len(cfg.Feeds))
	failed := false
	for _, feed := range cfg.Feeds {
		state, err := st.LoadState(feed.ID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot read %s state: %v\n", feed.ID, err)
			failed = true
			continue
		}
		feeds = append(feeds, struct {
			FeedID  string      `json:"feed_id"`
			URL     string      `json:"url"`
			Enabled bool        `json:"enabled"`
			State   store.State `json:"state"`
		}{feed.ID, feed.URL, feed.Enabled, state})
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(map[string]any{"schema_version": 1, "feeds": feeds}); err != nil {
		fmt.Fprintf(os.Stderr, "write status: %v\n", err)
		return 1
	}
	if failed {
		return 1
	}
	return 0
}

func archive(args []string) int {
	flags := flag.NewFlagSet("archive", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	configPath := flags.String("config", "config.json", "path to JSON configuration")
	repository := flags.String("repository", ".", "path inside the Git repository")
	remote := flags.String("remote", "origin", "Git remote to publish")
	branchPrefix := flags.String("branch-prefix", "rss/", "prefix for feed branches")
	onlyFeed := flags.String("feed", "", "archive one enabled feed ID")
	force := flags.Bool("force", false, "bypass poll and retry schedules")
	dryRun := flags.Bool("dry-run", false, "collect and create commits without pushing")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return 2
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	report, err := gitarchive.Publish(ctx, gitarchive.Options{
		Config:       cfg,
		RepoDir:      *repository,
		Remote:       *remote,
		BranchPrefix: *branchPrefix,
		OnlyFeed:     *onlyFeed,
		Force:        *force,
		DryRun:       *dryRun,
	})
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if report != nil {
		if encodeErr := encoder.Encode(report); encodeErr != nil {
			fmt.Fprintf(os.Stderr, "write report: %v\n", encodeErr)
			return 1
		}
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "archive failed: %v\n", err)
		if errors.Is(err, gitarchive.ErrConfiguration) {
			return 2
		}
		return 1
	}
	if report.BudgetReached {
		return 4
	}
	if report.Failed > 0 {
		return 1
	}
	return 0
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: rsscollector <validate|run|status|archive> --config config.json [options]")
}
