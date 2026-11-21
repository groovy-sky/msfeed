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

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
