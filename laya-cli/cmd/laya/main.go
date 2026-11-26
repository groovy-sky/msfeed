package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/groovy-sky/rsscollector/laya-cli/internal/systemone"
)

const (
	defaultModel = "/models/laya.gguf"
	defaultAddr  = "127.0.0.1:18080"
	startupLimit = 30 * time.Second
	requestLimit = 5 * time.Minute
	healthPoll   = 25 * time.Millisecond
)

var errServerExited = errors.New("llama-server exited before becoming ready")

type options struct {
	model         string
	addr          string
	raw           bool
	minConfidence float64
	timeout       time.Duration
}

type decisionOutput struct {
	Choice        string             `json:"choice"`
	Probabilities map[string]float64 `json:"probabilities"`
}

type lowConfidenceError struct {
	choice      string
	probability float64
	minimum     float64
}

func (e *lowConfidenceError) Error() string {
	return fmt.Sprintf(
		"choice %q confidence %.6g is below minimum %.6g",
		e.choice,
		e.probability,
		e.minimum,
	)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(execute(ctx, os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr))
}

func execute(
	ctx context.Context,
	args []string,
	lookupEnv func(string) (string, bool),
	stdout io.Writer,
	stderr io.Writer,
) int {
	config, choicesInput, state, err := parseOptions(args, lookupEnv, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	choices, err := systemone.ParseChoices(choicesInput)
	if err != nil {
		fmt.Fprintf(stderr, "laya: %v\n", err)
		return 2
	}
	baseURL, host, port, err := parseEndpoint(config.addr)
	if err != nil {
		fmt.Fprintf(stderr, "laya: %v\n", err)
		return 2
	}
	if strings.TrimSpace(state) == "" {
		fmt.Fprintln(stderr, "laya: state must not be empty")
		return 2
	}
	if math.IsNaN(config.minConfidence) ||
		math.IsInf(config.minConfidence, 0) ||
		config.minConfidence < 0 ||
		config.minConfidence > 1 {
		fmt.Fprintln(stderr, "laya: --min-confidence must be between 0 and 1")
		return 2
	}
	if config.timeout <= 0 {
		fmt.Fprintln(stderr, "laya: --timeout must be greater than zero")
		return 2
	}

	cleanup, err := ensureServer(ctx, baseURL, host, port, config.model, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "laya: %v\n", err)
		return 1
	}
	defer cleanup()

	client := systemone.NewClient(baseURL, &http.Client{Timeout: config.timeout})
	result, err := client.Decide(ctx, state, choices)
	if err != nil {
		fmt.Fprintf(stderr, "laya: %v\n", err)
		return 1
	}
	if result.Probability < config.minConfidence {
		var confidenceErr error = &lowConfidenceError{
			choice:      result.Choice,
			probability: result.Probability,
			minimum:     config.minConfidence,
		}
		fmt.Fprintf(stderr, "laya: %v\n", confidenceErr)
		return 2
	}

	if config.raw {
		if _, err := fmt.Fprintln(stdout, result.Choice); err != nil {
			fmt.Fprintf(stderr, "laya: write output: %v\n", err)
			return 1
		}
		return 0
	}
	output := decisionOutput{
		Choice:        result.Choice,
		Probabilities: result.Probabilities,
	}
	if err := json.NewEncoder(stdout).Encode(output); err != nil {
		fmt.Fprintf(stderr, "laya: encode output: %v\n", err)
		return 1
	}
	return 0
}

func parseOptions(
	args []string,
	lookupEnv func(string) (string, bool),
	stderr io.Writer,
) (options, string, string, error) {
	config := options{
		model: envValue(lookupEnv, "LAYA_MODEL", defaultModel),
		addr:  envValue(lookupEnv, "LAYA_ADDR", defaultAddr),
	}

	flags := flag.NewFlagSet("laya", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&config.model, "model", config.model, "path to the Laya GGUF model")
	flags.StringVar(&config.addr, "addr", config.addr, "llama-server host:port or base URL")
	flags.BoolVar(&config.raw, "raw", false, "print only the winning label")
	flags.DurationVar(&config.timeout, "timeout", requestLimit, "decision request timeout")
	flags.Float64Var(
		&config.minConfidence,
		"min-confidence",
		0,
		"minimum winning probability (0 to 1)",
	)
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: laya [flags] <choices-json> <state>")
		flags.PrintDefaults()
	}

	if err := flags.Parse(args); err != nil {
		return options{}, "", "", err
	}
	if flags.NArg() != 2 {
		flags.Usage()
		return options{}, "", "", errors.New("exactly two positional arguments are required")
	}
	return config, flags.Arg(0), flags.Arg(1), nil
}

func envValue(lookupEnv func(string) (string, bool), name string, fallback string) string {
	if value, ok := lookupEnv(name); ok && value != "" {
		return value
	}
	return fallback
}

func parseEndpoint(addr string) (baseURL string, host string, port string, err error) {
	raw := strings.TrimSpace(addr)
	if raw == "" {
		return "", "", "", errors.New("server address must not be empty")
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return "", "", "", fmt.Errorf("parse server address %q: %w", addr, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", "", "", fmt.Errorf("server address %q uses unsupported scheme %q", addr, parsed.Scheme)
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/") {
		return "", "", "", fmt.Errorf("server address %q must not contain credentials, a path, query, or fragment", addr)
	}
	host, port, err = net.SplitHostPort(parsed.Host)
	if err != nil {
		return "", "", "", fmt.Errorf("server address %q must include a port: %w", addr, err)
	}
	if host == "" {
		return "", "", "", fmt.Errorf("server address %q has an empty host", addr)
	}
	parsed.Path = ""
	return strings.TrimRight(parsed.String(), "/"), host, port, nil
}

func ensureServer(
	ctx context.Context,
	baseURL string,
	host string,
	port string,
	model string,
	stderr io.Writer,
) (func(), error) {
	healthy, status, err := probeHealth(ctx, baseURL, 100*time.Millisecond)
	if healthy {
		return func() {}, nil
	}
	if status == http.StatusServiceUnavailable {
		if err := waitForServer(ctx, baseURL, nil); err != nil {
			return nil, err
		}
		return func() {}, nil
	}
	if status != 0 || !errors.Is(err, syscall.ECONNREFUSED) {
		return nil, err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	modelPath := filepath.Clean(model)
	if info, statErr := os.Stat(modelPath); statErr != nil {
		return nil, fmt.Errorf("inspect model %q: %w", modelPath, statErr)
	} else if info.IsDir() {
		return nil, fmt.Errorf("model path %q is a directory", modelPath)
	}

	serverPath, err := exec.LookPath("llama-server")
	if err != nil {
		return nil, fmt.Errorf("find llama-server: %w", err)
	}
	// A one-shot server needs one slot with the entire context in a single batch.
	command := exec.CommandContext(
		ctx,
		serverPath,
		"-m", modelPath,
		"--host", host,
		"--port", port,
		"-c", "2048",
		"-b", "2048",
		"-ub", "2048",
		"--parallel", "1",
	)
	command.Env = os.Environ()
	// llama.cpp's automatic thread count does not account for CPU quotas.
	if os.Getenv("LLAMA_ARG_THREADS") == "" {
		command.Env = append(command.Env, "LLAMA_ARG_THREADS=1")
	}
	command.Stdout = stderr
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start llama-server: %w", err)
	}

	done := make(chan struct{})
	var processErr error
	go func() {
		processErr = command.Wait()
		close(done)
	}()
	cleanup := func() {
		select {
		case <-done:
			return
		default:
		}
		_ = command.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = command.Process.Kill()
			<-done
		}
	}

	if err := waitForServer(ctx, baseURL, done); err != nil {
		cleanup()
		if errors.Is(err, errServerExited) && processErr != nil {
			return nil, fmt.Errorf("%w: %w", err, processErr)
		}
		return nil, err
	}
	return cleanup, nil
}

func waitForServer(ctx context.Context, baseURL string, done <-chan struct{}) error {
	deadline := time.NewTimer(startupLimit)
	defer deadline.Stop()
	ticker := time.NewTicker(healthPoll)
	defer ticker.Stop()

	var lastHealthErr error
	for {
		healthy, status, healthErr := probeHealth(ctx, baseURL, 100*time.Millisecond)
		lastHealthErr = healthErr
		if healthy {
			select {
			case <-done:
				return errServerExited
			default:
				return nil
			}
		}
		if status != 0 && status != http.StatusServiceUnavailable {
			return healthErr
		}

		select {
		case <-done:
			if err := ctx.Err(); err != nil {
				return err
			}
			return errServerExited
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("llama-server did not become ready within %s (last health check: %v)", startupLimit, lastHealthErr)
		case <-ticker.C:
		}
	}
}

func probeHealth(
	ctx context.Context,
	baseURL string,
	timeout time.Duration,
) (healthy bool, status int, err error) {
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, baseURL+"/health", nil)
	if err != nil {
		return false, 0, fmt.Errorf("create health request: %w", err)
	}
	response, err := (&http.Client{Timeout: timeout}).Do(request)
	if err != nil {
		return false, 0, fmt.Errorf("probe health endpoint: %w", err)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	_ = response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return false, response.StatusCode, fmt.Errorf("llama-server health endpoint returned %s", response.Status)
	}
	return true, response.StatusCode, nil
}
