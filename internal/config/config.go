package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const SchemaVersion = 1

var feedIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`)
var headerNamePattern = regexp.MustCompile(`^[!#$%&'*+\-.^_` + "`" + `|~0-9A-Za-z]+$`)
var odataFieldPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Config struct {
	SchemaVersion  int      `json:"schema_version"`
	DataDir        string   `json:"data_dir"`
	MaxRunSeconds  int      `json:"max_run_seconds"`
	MaxFeedsPerRun int      `json:"max_feeds_per_run,omitempty"`
	HTTP           HTTP     `json:"http"`
	Retry          Retry    `json:"retry,omitempty"`
	Defaults       Defaults `json:"defaults"`
	Feeds          []Feed   `json:"feeds"`
	baseDir        string
}

type HTTP struct {
	TimeoutSeconds       int    `json:"timeout_seconds"`
	MaxBodyBytes         int64  `json:"max_body_bytes"`
	MaxRedirects         int    `json:"max_redirects"`
	MaxRequestsPerRun    int    `json:"max_requests_per_run,omitempty"`
	UserAgent            string `json:"user_agent"`
	AllowPrivateNetworks bool   `json:"allow_private_networks,omitempty"`
	PublicDNSFallback    bool   `json:"public_dns_fallback,omitempty"`
}

type Retry struct {
	MaxAttempts           int `json:"max_attempts,omitempty"`
	InitialBackoffSeconds int `json:"initial_backoff_seconds,omitempty"`
	MaxBackoffSeconds     int `json:"max_backoff_seconds,omitempty"`
	JitterPercent         int `json:"jitter_percent,omitempty"`
}

type Defaults struct {
	PollIntervalSeconds int `json:"poll_interval_seconds"`
	MaxEntriesPerRun    int `json:"max_entries_per_run"`
}

type Feed struct {
	ID                  string          `json:"id"`
	URL                 string          `json:"url"`
	Enabled             bool            `json:"enabled"`
	PollIntervalSeconds int             `json:"poll_interval_seconds,omitempty"`
	MaxEntriesPerRun    int             `json:"max_entries_per_run,omitempty"`
	Authentication      *Authentication `json:"authentication,omitempty"`
	Enrichment          *Enrichment     `json:"enrichment,omitempty"`
}

type Authentication struct {
	HeaderName string `json:"header_name"`
	ValueEnv   string `json:"value_env,omitempty"`
	ValueFile  string `json:"value_file,omitempty"`
}

type Enrichment struct {
	Provider           string `json:"provider"`
	URL                string `json:"url"`
	IDField            string `json:"id_field,omitempty"`
	ContentField       string `json:"content_field,omitempty"`
	ContentType        string `json:"content_type,omitempty"`
	MaxItemsPerRequest int    `json:"max_items_per_request,omitempty"`
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
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve config path: %w", err)
	}
	cfg.baseDir = filepath.Dir(absolutePath)
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
	if c.MaxFeedsPerRun < 0 {
		return errors.New("max_feeds_per_run cannot be negative")
	}
	if c.MaxFeedsPerRun > 10000 {
		return errors.New("max_feeds_per_run is too large")
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
	if c.HTTP.MaxRequestsPerRun < 0 {
		return errors.New("http.max_requests_per_run cannot be negative")
	}
	if c.HTTP.MaxRequestsPerRun == 0 {
		c.HTTP.MaxRequestsPerRun = 100
	}
	if c.HTTP.MaxRequestsPerRun > 10000 {
		return errors.New("http.max_requests_per_run is too large")
	}
	if c.HTTP.MaxRedirects < 0 || c.HTTP.MaxRedirects > 20 {
		return errors.New("http.max_redirects must be between 0 and 20")
	}
	if c.HTTP.UserAgent == "" {
		c.HTTP.UserAgent = "rsscollector/1.0"
	}
	if err := c.Retry.applyDefaultsAndValidate(); err != nil {
		return err
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
		if feed.PollIntervalSeconds > 86400*365 || feed.MaxEntriesPerRun > 100000 {
			return fmt.Errorf("feed %q poll interval or entry limit is too large", feed.ID)
		}
		if err := validateAuthentication(feed.Authentication); err != nil {
			return fmt.Errorf("feed %q authentication: %w", feed.ID, err)
		}
		if err := validateEnrichment(feed.Enrichment); err != nil {
			return fmt.Errorf("feed %q enrichment: %w", feed.ID, err)
		}
	}
	if len(c.Feeds) == 0 {
		return errors.New("at least one feed is required")
	}
	return nil
}

func (r *Retry) applyDefaultsAndValidate() error {
	if r.MaxAttempts < 0 || r.InitialBackoffSeconds < 0 || r.MaxBackoffSeconds < 0 || r.JitterPercent < 0 {
		return errors.New("retry limits cannot be negative")
	}
	if r.MaxAttempts == 0 {
		r.MaxAttempts = 5
	}
	if r.InitialBackoffSeconds == 0 {
		r.InitialBackoffSeconds = 30
	}
	if r.MaxBackoffSeconds == 0 {
		r.MaxBackoffSeconds = 3600
	}
	if r.JitterPercent == 0 {
		r.JitterPercent = 20
	}
	if r.MaxAttempts > 20 || r.InitialBackoffSeconds > 3600 || r.MaxBackoffSeconds > 86400 || r.JitterPercent > 50 {
		return errors.New("configured retry limit is too large")
	}
	if r.InitialBackoffSeconds > r.MaxBackoffSeconds {
		return errors.New("retry.initial_backoff_seconds cannot exceed retry.max_backoff_seconds")
	}
	return nil
}

func validateAuthentication(auth *Authentication) error {
	if auth == nil {
		return nil
	}
	if !headerNamePattern.MatchString(auth.HeaderName) {
		return errors.New("header_name is invalid")
	}
	switch strings.ToLower(auth.HeaderName) {
	case "host", "content-length", "transfer-encoding", "connection", "upgrade":
		return errors.New("header_name is not allowed")
	}
	if (auth.ValueEnv == "") == (auth.ValueFile == "") {
		return errors.New("exactly one of value_env or value_file is required")
	}
	return nil
}

func validateEnrichment(enrichment *Enrichment) error {
	if enrichment == nil {
		return nil
	}
	if enrichment.Provider != "odata" {
		return fmt.Errorf("unsupported provider %q", enrichment.Provider)
	}
	if err := validateURL(enrichment.URL); err != nil {
		return err
	}
	if enrichment.IDField == "" {
		enrichment.IDField = "id"
	}
	if enrichment.ContentField == "" {
		enrichment.ContentField = "description"
	}
	if enrichment.ContentType == "" {
		enrichment.ContentType = "text/html"
	}
	if enrichment.MaxItemsPerRequest == 0 {
		enrichment.MaxItemsPerRequest = 100
	}
	if !odataFieldPattern.MatchString(enrichment.IDField) {
		return errors.New("id_field is invalid")
	}
	if !odataFieldPattern.MatchString(enrichment.ContentField) {
		return errors.New("content_field is invalid")
	}
	switch enrichment.ContentType {
	case "text", "text/plain", "html", "text/html", "application/xhtml+xml":
	default:
		return fmt.Errorf("unsupported content_type %q", enrichment.ContentType)
	}
	if enrichment.MaxItemsPerRequest < 1 || enrichment.MaxItemsPerRequest > 500 {
		return errors.New("max_items_per_request must be between 1 and 500")
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

func (c Config) ResolveAuthentication(feed Feed) (map[string]string, error) {
	if feed.Authentication == nil {
		return nil, nil
	}
	auth := feed.Authentication
	var value string
	switch {
	case auth.ValueEnv != "":
		value = os.Getenv(auth.ValueEnv)
		if value == "" {
			return nil, fmt.Errorf("environment variable %s is unset or empty", auth.ValueEnv)
		}
	case auth.ValueFile != "":
		path := auth.ValueFile
		if !filepath.IsAbs(path) {
			path = filepath.Join(c.baseDir, path)
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("stat authentication file: %w", err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
			return nil, errors.New("authentication file must be a regular file readable only by its owner")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read authentication file: %w", err)
		}
		value = strings.TrimSpace(string(data))
		if value == "" {
			return nil, errors.New("authentication file is empty")
		}
	}
	return map[string]string{auth.HeaderName: value}, nil
}
