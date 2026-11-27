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
	AreaDataAndDatabases            = "Data & Databases"
	AreaAIAndAgents                 = "AI & Agents"
	AreaNetworkingAndConnectivity   = "Networking & Connectivity"
	AreaServerless                  = "Serverless"
	AreaSecurityAndIdentity         = "Security & Identity"
	AreaIntegrationAndAPIs          = "Integration & APIs"
	AreaComputeAndVirtualization    = "Compute & Virtualization"
	AreaStorageAndBackup            = "Storage & Backup"
	AreaManagementAndResiliency     = "Management & Resiliency"
	AreaContainersAndKubernetes     = "Containers & Kubernetes"
	AreaSovereigntyAndCompliance    = "Sovereignty & Compliance"
	AreaMarketplaceAndCommercial    = "Marketplace & Commercial"
	AreaBusinessApplications        = "Business Applications"
	AreaWebAndAppPlatform           = "Web & App Platform"
	AreaHybridAndMulticloud         = "Hybrid & Multicloud"
	AreaAIInfrastructureAndCompute  = "AI Infrastructure & Compute"
	AreaAIAgentEngineering          = "AI & Agent Engineering"
	AreaSovereigntyAndCloudStrategy = "Sovereignty & Cloud Strategy"
	AreaCorporateAndPeople          = "Corporate & People"
	AreaDeveloperToolsAndData       = "Developer Tools & Data"
	AreaOther                       = "Other"
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
		AreaDataAndDatabases:            "Relational, analytical, or other databases and data platforms.",
		AreaAIAndAgents:                 "AI services, copilots, models, or autonomous agents.",
		AreaNetworkingAndConnectivity:   "Networking, connectivity, routing, or traffic delivery.",
		AreaServerless:                  "Serverless compute and event-driven hosting.",
		AreaSecurityAndIdentity:         "Security, identity, access, authentication, or authorization.",
		AreaIntegrationAndAPIs:          "Application integration, messaging, APIs, and API management.",
		AreaComputeAndVirtualization:    "Virtual machines, compute infrastructure, or virtualization.",
		AreaStorageAndBackup:            "Storage, data protection, backup, restore, or archival.",
		AreaManagementAndResiliency:     "Cloud operations, monitoring, management, availability, or recovery.",
		AreaContainersAndKubernetes:     "Container platforms, registries, Kubernetes, and orchestration.",
		AreaSovereigntyAndCompliance:    "Regulatory compliance, data sovereignty, or residency controls.",
		AreaMarketplaceAndCommercial:    "Marketplace, pricing, offers, billing, or commercial terms.",
		AreaBusinessApplications:        "Business applications and enterprise productivity workloads.",
		AreaWebAndAppPlatform:           "Web hosting and application platforms.",
		AreaHybridAndMulticloud:         "Hybrid cloud and multicloud services or management.",
		AreaAIInfrastructureAndCompute:  "Infrastructure and compute dedicated to AI model workloads.",
		AreaAIAgentEngineering:          "Developer tooling, frameworks, evaluation, or engineering for AI agents.",
		AreaSovereigntyAndCloudStrategy: "Strategic cloud sovereignty, adoption, policy, or architecture.",
		AreaCorporateAndPeople:          "Corporate news, organization, leadership, workforce, or people.",
		AreaDeveloperToolsAndData:       "Developer tooling and developer-focused data products.",
		AreaOther:                       "The item does not fit another listed technical or business area.",
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
	case AreaDataAndDatabases, AreaAIAndAgents, AreaNetworkingAndConnectivity,
		AreaServerless, AreaSecurityAndIdentity, AreaIntegrationAndAPIs,
		AreaComputeAndVirtualization, AreaStorageAndBackup, AreaManagementAndResiliency,
		AreaContainersAndKubernetes, AreaSovereigntyAndCompliance,
		AreaMarketplaceAndCommercial, AreaBusinessApplications, AreaWebAndAppPlatform,
		AreaHybridAndMulticloud, AreaAIInfrastructureAndCompute, AreaAIAgentEngineering,
		AreaSovereigntyAndCloudStrategy, AreaCorporateAndPeople,
		AreaDeveloperToolsAndData, AreaOther:
		return true
	default:
		return false
	}
}
