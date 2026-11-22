package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := writeConfig(t, `{"schema_version":1,"data_dir":"data","feeds":[{"id":"x","url":"https://example.com/feed","enabled":true}],"unknown":true}`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("error = %v, want unknown-field error", err)
	}
}

func TestValidateRejectsUnsafeFeedID(t *testing.T) {
	cfg := Config{
		SchemaVersion: 1, DataDir: "data",
		Feeds: []Feed{{ID: "../escape", URL: "https://example.com/feed", Enabled: true}},
	}
	if err := cfg.ApplyDefaultsAndValidate(); err == nil {
		t.Fatal("unsafe feed ID accepted")
	}
}

func TestValidateRejectsNegativeLimits(t *testing.T) {
	cfg := Config{
		SchemaVersion: 1, DataDir: "data", MaxRunSeconds: -1,
		Feeds: []Feed{{ID: "example", URL: "https://example.com/feed", Enabled: true}},
	}

	if err := cfg.ApplyDefaultsAndValidate(); err == nil {
		t.Fatal("negative limit accepted")
	}
}

func TestValidateAppliesODataEnrichmentDefaults(t *testing.T) {
	cfg := Config{
		SchemaVersion: 1, DataDir: "data",
		Feeds: []Feed{{
			ID: "example", URL: "https://example.com/feed", Enabled: true,
			Enrichment: &Enrichment{
				Provider: "odata", URL: "https://example.com/api/updates",
			},
		}},
	}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	enrichment := cfg.Feeds[0].Enrichment
	if enrichment.IDField != "id" || enrichment.ContentField != "description" ||
		enrichment.ContentType != "text/html" || enrichment.MaxItemsPerRequest != 100 {
		t.Fatalf("enrichment defaults = %+v", enrichment)
	}
}

func TestValidateRejectsInvalidEnrichment(t *testing.T) {
	base := Enrichment{
		Provider: "odata", URL: "https://example.com/api",
		IDField: "id", ContentField: "description", ContentType: "text/html",
		MaxItemsPerRequest: 10,
	}
	tests := []struct {
		name   string
		mutate func(*Enrichment)
	}{
		{"provider", func(value *Enrichment) { value.Provider = "mcp" }},
		{"url", func(value *Enrichment) { value.URL = "file:///tmp/feed" }},
		{"id field", func(value *Enrichment) { value.IDField = "id or true" }},
		{"content field", func(value *Enrichment) { value.ContentField = "description/$value" }},
		{"content type", func(value *Enrichment) { value.ContentType = "application/json" }},
		{"batch size", func(value *Enrichment) { value.MaxItemsPerRequest = 501 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enrichment := base
			tt.mutate(&enrichment)
			cfg := Config{
				SchemaVersion: 1, DataDir: "data",
				Feeds: []Feed{{
					ID: "example", URL: "https://example.com/feed",
					Enabled: true, Enrichment: &enrichment,
				}},
			}
			if err := cfg.ApplyDefaultsAndValidate(); err == nil {
				t.Fatalf("invalid enrichment accepted: %+v", enrichment)
			}
		})
	}
}

func TestResolveAuthenticationFromProtectedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("secret-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	feed := Feed{Authentication: &Authentication{
		HeaderName: "Authorization", ValueFile: path,
	}}
	headers, err := (Config{}).ResolveAuthentication(feed)
	if err != nil {
		t.Fatal(err)
	}
	if headers["Authorization"] != "secret-value" {
		t.Fatalf("authorization = %q", headers["Authorization"])
	}
}

func TestResolveAuthenticationRejectsSharedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("secret-value"), 0o644); err != nil {
		t.Fatal(err)
	}
	feed := Feed{Authentication: &Authentication{HeaderName: "Authorization", ValueFile: path}}
	if _, err := (Config{}).ResolveAuthentication(feed); err == nil {
		t.Fatal("shared-secret file was accepted")
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
