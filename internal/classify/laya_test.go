package classify

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/groovy-sky/msfeed/internal/parse"
)

func TestLayaClassifyUsesFixedStatesAndAreasAndStableInput(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test executable is a shell script")
	}
	dir := t.TempDir()
	arguments := filepath.Join(dir, "arguments")
	executable := filepath.Join(dir, "laya")
	script := `#!/bin/sh
printf '%s\n' "$@" >> "$LAYA_TEST_ARGUMENTS"
case "$2" in
  *Monitoring*) printf 'Monitoring\n' ;;
  *) printf 'Preview\n' ;;
esac
`
	if err := os.WriteFile(executable, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LAYA_TEST_ARGUMENTS", arguments)

	classifier, err := NewLaya(executable)
	if err != nil {
		t.Fatal(err)
	}
	entry := parse.Entry{
		Title:   "Private Preview: Example",
		Summary: "  A newly   available private preview. ",
	}
	result, err := classifier.Classify(context.Background(), entry)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != StatePreview || result.Area != AreaMonitoring ||
		result.InputSHA256 != InputHash(Input(entry)) {
		t.Fatalf("result = %+v", result)
	}
	args, err := os.ReadFile(arguments)
	if err != nil {
		t.Fatal(err)
	}
	wantArgs := "--raw\n" + choicesJSON + "\n" + Input(entry) + "\n" +
		"--raw\n" + areaChoicesJSON + "\n" + Input(entry) + "\n"
	if string(args) != wantArgs {
		t.Fatalf("arguments = %q", args)
	}
	for _, state := range []string{
		StateGenerallyAvailable, StatePreview, StateRetirement, StateOther,
	} {
		if !strings.Contains(choicesJSON, state) {
			t.Fatalf("choices do not contain %q: %s", state, choicesJSON)
		}
	}
	expectedAreas := []string{
		AreaData, AreaAI, AreaCompute, AreaNetworking, AreaApplications,
		AreaIntegration, AreaSecurity, AreaStorage, AreaMonitoring,
		AreaOperations, AreaDevelopment, AreaHybrid, AreaSovereignty,
		AreaBusiness, AreaCommercial, AreaCorporate, AreaOther,
	}
	var areas map[string]string
	if err := json.Unmarshal([]byte(areaChoicesJSON), &areas); err != nil {
		t.Fatal(err)
	}
	if len(areas) != len(expectedAreas) {
		t.Fatalf("area count = %d, want %d: %s", len(areas), len(expectedAreas), areaChoicesJSON)
	}
	for _, area := range expectedAreas {
		if _, ok := areas[area]; !ok {
			t.Fatalf("area choices do not contain %q: %s", area, areaChoicesJSON)
		}
	}
}

func TestLayaClassifyRejectsUnexpectedOutput(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test executable is a shell script")
	}

	executable := filepath.Join(t.TempDir(), "laya")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nprintf 'Launched\\n'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	classifier, err := NewLaya(executable)
	if err != nil {
		t.Fatal(err)
	}
	_, err = classifier.Classify(context.Background(), parse.Entry{Title: "An update"})
	if err == nil || !strings.Contains(err.Error(), "invalid state") {
		t.Fatalf("error = %v", err)
	}
}

func TestLayaClassifyRejectsUnexpectedArea(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test executable is a shell script")
	}
	executable := filepath.Join(t.TempDir(), "laya")
	script := `#!/bin/sh
case "$2" in
  *Monitoring*) printf 'Storage & Backup\n' ;;
  *) printf 'Preview\n' ;;
esac
`
	if err := os.WriteFile(executable, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	classifier, err := NewLaya(executable)
	if err != nil {
		t.Fatal(err)
	}
	_, err = classifier.Classify(context.Background(), parse.Entry{Title: "An update"})
	if err == nil || !strings.Contains(err.Error(), "invalid area") {
		t.Fatalf("error = %v", err)
	}
}
