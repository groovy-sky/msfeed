package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

const SchemaVersion = 1

var feedIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`)

type Config struct {
	SchemaVersion int      `json:"schema_version"`
	DataDir       string   `json:"data_dir"`
	MaxRunSeconds int      `json:"max_run_seconds"`
	HTTP          HTTP     `json:"http"`
	Defaults      Defaults `json:"defaults"`
	Feeds         []Feed   `json:"feeds"`
}

type HTTP struct {
	TimeoutSeconds       int    `json:"timeout_seconds"`
	MaxBodyBytes         int64  `json:"max_body_bytes"`
	MaxRedirects         int    `json:"max_redirects"`
	UserAgent            string `json:"user_agent"`
	AllowPrivateNetworks bool   `json:"allow_private_networks,omitempty"`
}

type Defaults struct {
	PollIntervalSeconds int `json:"poll_interval_seconds"`
	MaxEntriesPerRun    int `json:"max_entries_per_run"`
}

type Feed struct {
	ID                  string `json:"id"`
	URL                 string `json:"url"`
	Enabled             bool   `json:"enabled"`
	PollIntervalSeconds int    `json:"poll_interval_seconds,omitempty"`
	MaxEntriesPerRun    int    `json:"max_entries_per_run,omitempty"`
}

func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	var cfg Config
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("config contains multiple JSON values")
	}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) ApplyDefaultsAndValidate() error {
	if c.SchemaVersion != SchemaVersion {
		return fmt.Errorf("schema_version must be %d", SchemaVersion)
	}
	if strings.TrimSpace(c.DataDir) == "" {
		return errors.New("data_dir is required")
	}
	if c.MaxRunSeconds < 0 {
		return errors.New("max_run_seconds cannot be negative")
	}
	if c.MaxRunSeconds == 0 {
		c.MaxRunSeconds = 300
	}
	if c.HTTP.TimeoutSeconds < 0 {
		return errors.New("http.timeout_seconds cannot be negative")
	}
	if c.HTTP.TimeoutSeconds == 0 {
		c.HTTP.TimeoutSeconds = 30
	}
	if c.HTTP.MaxBodyBytes < 0 {
		return errors.New("http.max_body_bytes cannot be negative")
	}
	if c.HTTP.MaxBodyBytes == 0 {
		c.HTTP.MaxBodyBytes = 8 << 20
	}
	if c.HTTP.MaxRedirects < 0 || c.HTTP.MaxRedirects > 20 {
		return errors.New("http.max_redirects must be between 0 and 20")
	}
	if c.HTTP.UserAgent == "" {
		c.HTTP.UserAgent = "rsscollector/1.0"
	}
	if c.Defaults.PollIntervalSeconds < 0 {
		return errors.New("defaults.poll_interval_seconds cannot be negative")
	}
	if c.Defaults.PollIntervalSeconds == 0 {
		c.Defaults.PollIntervalSeconds = 3600
	}
	if c.Defaults.MaxEntriesPerRun < 0 {
		return errors.New("defaults.max_entries_per_run cannot be negative")
	}
	if c.Defaults.MaxEntriesPerRun == 0 {
		c.Defaults.MaxEntriesPerRun = 500
	}
	if c.MaxRunSeconds > 86400 || c.HTTP.TimeoutSeconds > 300 || c.HTTP.MaxBodyBytes > 64<<20 {
		return errors.New("configured runtime, HTTP timeout, or body limit is too large")
	}
	if c.Defaults.PollIntervalSeconds > 86400*365 || c.Defaults.MaxEntriesPerRun > 100000 {
		return errors.New("configured poll interval or entry limit is too large")
	}
	ids := make(map[string]struct{}, len(c.Feeds))
	for i := range c.Feeds {
		feed := &c.Feeds[i]
		if !feedIDPattern.MatchString(feed.ID) {
			return fmt.Errorf("feed %q has an unsafe or invalid id", feed.ID)
		}
		if _, ok := ids[feed.ID]; ok {
			return fmt.Errorf("feed id %q is duplicated", feed.ID)
		}
		ids[feed.ID] = struct{}{}
		if err := validateURL(feed.URL); err != nil {
			return fmt.Errorf("feed %q: %w", feed.ID, err)
		}
		if feed.PollIntervalSeconds < 0 || feed.MaxEntriesPerRun < 0 {
			return fmt.Errorf("feed %q has a negative limit", feed.ID)
		}
		if feed.PollIntervalSeconds == 0 {
			feed.PollIntervalSeconds = c.Defaults.PollIntervalSeconds
		}
		if feed.MaxEntriesPerRun == 0 {
			feed.MaxEntriesPerRun = c.Defaults.MaxEntriesPerRun
		}
	}
	if len(c.Feeds) == 0 {
		return errors.New("at least one feed is required")
	}
	return nil
}

func validateURL(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errors.New("url is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("url scheme must be http or https")
	}
	if parsed.Host == "" {
		return errors.New("url host is required")
	}
	if parsed.User != nil {
		return errors.New("url must not contain credentials")
	}
	return nil
}

func (c Config) RunTimeout() time.Duration {
	return time.Duration(c.MaxRunSeconds) * time.Second
}

func (c Config) HTTPTimeout() time.Duration {
	return time.Duration(c.HTTP.TimeoutSeconds) * time.Second
}
