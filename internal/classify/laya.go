package classify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"unicode/utf8"

	"github.com/groovy-sky/msfeed/internal/parse"
)

const (
	ProviderLaya  = "laya-cli"
	maxInputBytes = 4096
)

const (
	StateGenerallyAvailable = "Generally Available"
	StatePreview            = "Preview"
	StateRetirement         = "Retirement"
	StateOther              = "Other"
)

var choices = map[string]string{
	StateGenerallyAvailable: "The update says a service or feature is generally available, launched, or ready for production use.",
	StatePreview:            "The update says a service or feature is in public, private, limited, or beta preview.",
	StateRetirement:         "The update announces retirement, end of support, deprecation, removal, or shutdown.",
	StateOther:              "The update is an announcement, editorial article, or other item that is not a lifecycle status.",
}

var choicesJSON = func() string {
	data, err := json.Marshal(choices)
	if err != nil {
		panic("classification choices must marshal: " + err.Error())
	}
	return string(data)
}()

type Result struct {
	State       string
	InputSHA256 string
}

type Classifier interface {
	Provider() string
	Classify(context.Context, parse.Entry) (Result, error)
}

type Laya struct {
	executable string
}

func NewLaya(executable string) (*Laya, error) {
	executable = strings.TrimSpace(executable)
	if executable == "" {
		return nil, errors.New("laya executable is required")
	}
	resolved, err := exec.LookPath(executable)
	if err != nil {
		return nil, fmt.Errorf("find laya executable %q: %w", executable, err)
	}
	return &Laya{executable: resolved}, nil
}

func (*Laya) Provider() string {
	return ProviderLaya
}

func (l *Laya) Classify(ctx context.Context, entry parse.Entry) (Result, error) {
	input := Input(entry)
	if input == "" {
		return Result{}, errors.New("classification input is empty")
	}

	command := exec.CommandContext(
		ctx,
		l.executable,
		"--raw",
		choicesJSON,
		input,
	)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Result{}, ctxErr
		}
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			return Result{}, fmt.Errorf("run laya-cli: %w", err)
		}
		return Result{}, fmt.Errorf("run laya-cli: %w: %s", err, message)
	}

	state := strings.TrimSpace(stdout.String())
	if !ValidState(state) {
		return Result{}, fmt.Errorf("laya-cli returned invalid state %q", state)
	}
	return Result{State: state, InputSHA256: InputHash(input)}, nil
}

func Input(entry parse.Entry) string {
	parts := make([]string, 0, 3)
	appendField := func(name, value string) {
		value = strings.Join(strings.Fields(value), " ")
		if value != "" {
			parts = append(parts, name+": "+value)
		}
	}
	appendField("Title", entry.Title)
	appendField("Summary", entry.Summary)
	if entry.Content != "" && entry.Content != entry.Summary {
		appendField("Content", entry.Content)
	}
	input := strings.Join(parts, "\n")
	if len(input) > maxInputBytes {
		input = input[:maxInputBytes]
		for !utf8.ValidString(input) {
			input = input[:len(input)-1]
		}
	}
	return strings.TrimSpace(input)
}

func InputHash(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

func ValidState(state string) bool {
	switch state {
	case StateGenerallyAvailable, StatePreview, StateRetirement, StateOther:
		return true
	default:
		return false
	}
}
