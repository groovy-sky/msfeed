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

const (
	AreaData         = "Data"
	AreaAI           = "AI"
	AreaCompute      = "Compute"
	AreaNetworking   = "Networking"
	AreaApplications = "Applications"
	AreaIntegration  = "Integration"
	AreaSecurity     = "Security"
	AreaStorage      = "Storage"
	AreaMonitoring   = "Monitoring"
	AreaOperations   = "Operations"
	AreaDevelopment  = "Development"
	AreaHybrid       = "Hybrid"
	AreaSovereignty  = "Sovereignty"
	AreaBusiness     = "Business"
	AreaCommercial   = "Commercial"
	AreaCorporate    = "Corporate"
	AreaOther        = "Other"
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

var areaChoicesJSON = func() string {
	data, err := json.Marshal(map[string]string{
		AreaData:         "Data platforms, databases, analytics, or data engineering.",
		AreaAI:           "Core AI models, inference, copilots, or agent capabilities and engineering.",
		AreaCompute:      "Virtual machines, compute infrastructure, or virtualization.",
		AreaNetworking:   "Networking, connectivity, routing, or traffic delivery.",
		AreaApplications: "Business, enterprise, or user-facing applications.",
		AreaIntegration:  "Application integration, messaging, APIs, or API management.",
		AreaSecurity:     "Security, identity, authentication, or authorization.",
		AreaStorage:      "Storage, data protection, backup, restore, or archival.",
		AreaMonitoring:   "Monitoring, operational insights, observability, traces, diagnostics, telemetry, or recurring failure analysis.",
		AreaOperations:   "Cloud operations, management, reliability, or recovery.",
		AreaDevelopment:  "Developer tools, SDKs, frameworks, testing, or engineering workflows.",
		AreaHybrid:       "Hybrid, multicloud, edge, or cross-environment services.",
		AreaSovereignty:  "Data sovereignty, residency, compliance, or regulatory controls.",
		AreaBusiness:     "Business processes, productivity, or organizational solutions.",
		AreaCommercial:   "Marketplace, pricing, billing, offers, or commercial terms.",
		AreaCorporate:    "Corporate news, leadership, workforce, or people.",
		AreaOther:        "The item does not fit another listed area.",
	})
	if err != nil {
		panic("area choices must marshal: " + err.Error())
	}
	return string(data)
}()

type Result struct {
	State       string
	Area        string
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

	state, err := l.selectChoice(ctx, choicesJSON, input, ValidState, "state")
	if err != nil {
		return Result{}, err
	}
	area, err := l.selectChoice(ctx, areaChoicesJSON, input, ValidArea, "area")
	if err != nil {
		return Result{}, err
	}
	return Result{State: state, Area: area, InputSHA256: InputHash(input)}, nil
}

func (l *Laya) selectChoice(
	ctx context.Context,
	choicesJSON, input string,
	valid func(string) bool,
	field string,
) (string, error) {
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
			return "", ctxErr
		}
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			return "", fmt.Errorf("run laya-cli for %s: %w", field, err)
		}
		return "", fmt.Errorf("run laya-cli for %s: %w: %s", field, err, message)
	}

	choice := strings.TrimSpace(stdout.String())
	if !valid(choice) {
		return "", fmt.Errorf("laya-cli returned invalid %s %q", field, choice)
	}
	return choice, nil
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

func ValidArea(area string) bool {
	switch area {
	case AreaData, AreaAI, AreaCompute, AreaNetworking, AreaApplications,
		AreaIntegration, AreaSecurity, AreaStorage, AreaMonitoring,
		AreaOperations, AreaDevelopment, AreaHybrid, AreaSovereignty,
		AreaBusiness, AreaCommercial, AreaCorporate, AreaOther:
		return true
	default:
		return false
	}
}
