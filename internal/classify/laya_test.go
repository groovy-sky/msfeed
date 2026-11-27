package classify

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/groovy-sky/msfeed/internal/parse"
)

func TestLayaClassifyUsesFixedStatesAndStableInput(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test executable is a shell script")
	}
	dir := t.TempDir()
	arguments := filepath.Join(dir, "arguments")
	executable := filepath.Join(dir, "laya")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$LAYA_TEST_ARGUMENTS\"\nprintf 'Preview\\n'\n"
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
	if result.State != StatePreview || result.InputSHA256 != InputHash(Input(entry)) {
		t.Fatalf("result = %+v", result)
	}
	args, err := os.ReadFile(arguments)
	if err != nil {
		t.Fatal(err)
	}
	if string(args) != "--raw\n"+choicesJSON+"\n"+Input(entry)+"\n" {
		t.Fatalf("arguments = %q", args)
	}
	for _, state := range []string{
		StateGenerallyAvailable, StatePreview, StateRetirement, StateOther,
	} {
		if !strings.Contains(choicesJSON, state) {
			t.Fatalf("choices do not contain %q: %s", state, choicesJSON)
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
