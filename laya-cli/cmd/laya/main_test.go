package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/groovy-sky/rsscollector/laya-cli/internal/systemone"
)

func TestExecuteRawWithWarmServer(t *testing.T) {
	server := decisionServer(t, map[string]float64{
		"joke":   0.004,
		"story":  0.009,
		"report": 0.987,
	})
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := execute(
		context.Background(),
		[]string{"--raw", `["joke","story","report"]`, "backup completed"},
		environment(map[string]string{"LAYA_ADDR": server.URL}),
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if stdout.String() != "report\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestExecuteRawReportsOutputFailure(t *testing.T) {
	server := decisionServer(t, map[string]float64{"report": 1})
	defer server.Close()

	var stderr bytes.Buffer
	code := execute(context.Background(),
		[]string{"--raw", `["report"]`, "backup completed"},
		environment(map[string]string{"LAYA_ADDR": server.URL}), errorWriter{}, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "write output: stdout failed") {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, errors.New("stdout failed")
}

func TestExecuteHelp(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := execute(
		context.Background(),
		[]string{"--help"},
		environment(nil),
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	if !strings.Contains(stderr.String(), "Usage: laya") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestExecuteStructuredOutput(t *testing.T) {
	server := decisionServer(t, map[string]float64{"report": 0.987})
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := execute(
		context.Background(),
		[]string{`{"report":"A factual status log"}`, "backup completed"},
		environment(map[string]string{"LAYA_ADDR": server.URL}),
		&stdout,
		&stderr,
	)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	var output decisionOutput
	if err := json.NewDecoder(&stdout).Decode(&output); err != nil {
		t.Fatal(err)
	}
	if output.Choice != "report" || output.Probabilities["report"] != 0.987 {
		t.Fatalf("output = %#v", output)
	}
}

func TestExecuteRejectsLowConfidence(t *testing.T) {
	server := decisionServer(t, map[string]float64{"report": 0.5, "other": 0.5})
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := execute(
		context.Background(),
		[]string{"--min-confidence", "0.75", `["report","other"]`, "unknown state"},
		environment(map[string]string{"LAYA_ADDR": server.URL}),
		&stdout,
		&stderr,
	)

	if code != 2 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "below minimum") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestExecuteWaitsForWarmServer(t *testing.T) {
	var healthChecks atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/health":
			if healthChecks.Add(1) <= 2 {
				http.Error(response, "loading model", http.StatusServiceUnavailable)
				return
			}
			response.WriteHeader(http.StatusOK)
		case "/v1/systemone":
			_, _ = response.Write([]byte(`{"answers":{"decision":{"probabilities":{"report":0.9,"other":0.1}}}}`))
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := execute(context.Background(),
		[]string{"--raw", `["report","other"]`, "backup completed"},
		environment(map[string]string{"LAYA_ADDR": server.URL}), &stdout, &stderr)
	if code != 0 || stdout.String() != "report\n" {
		t.Fatalf("exit code = %d, stdout = %q, stderr = %q", code, stdout.String(), stderr.String())
	}
	if healthChecks.Load() < 3 {
		t.Fatalf("health checks = %d, want at least 3", healthChecks.Load())
	}
}

func TestExecuteReportsHealthTimeoutWithoutSpawning(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		select {
		case <-request.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)

	var stdout, stderr bytes.Buffer
	code := execute(context.Background(),
		[]string{"--model", filepath.Join(t.TempDir(), "missing.gguf"), `["report","other"]`, "backup completed"},
		environment(map[string]string{"LAYA_ADDR": server.URL}), &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "probe health endpoint") {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "inspect model") {
		t.Fatalf("attempted to start a local server after health timeout: %s", stderr.String())
	}
}

func TestExecuteRequestTimeout(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/health" {
			response.WriteHeader(http.StatusOK)
			return
		}
		select {
		case <-request.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)

	var stdout, stderr bytes.Buffer
	code := execute(context.Background(),
		[]string{"--timeout", "50ms", `["report","other"]`, "backup completed"},
		environment(map[string]string{"LAYA_ADDR": server.URL}), &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "deadline exceeded") {
		t.Fatalf("exit code = %d, stdout = %q, stderr = %q", code, stdout.String(), stderr.String())
	}
}

func TestExecuteRejectsNonPositiveTimeout(t *testing.T) {
	for _, timeout := range []string{"0s", "-1s"} {
		t.Run(timeout, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := execute(context.Background(),
				[]string{"--timeout", timeout, `["report","other"]`, "backup completed"},
				environment(nil), &stdout, &stderr)
			if code != 2 || !strings.Contains(stderr.String(), "--timeout must be greater than zero") {
				t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
			}
		})
	}
}

func TestExecuteColdServerHandlesLongInputAndCleansUp(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the test server launcher uses a shell")
	}

	for _, tt := range []struct {
		name        string
		threads     string
		wantThreads string
		stall       bool
	}{
		{name: "default", wantThreads: "1"},
		{name: "explicit override", threads: "2", wantThreads: "2"},
		{name: "request timeout", wantThreads: "1", stall: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			launcher := filepath.Join(dir, "llama-server")
			script := "#!/bin/sh\nexec \"$LAYA_TEST_BINARY\" '-test.run=^TestLlamaServerHelper$' -- \"$@\"\n"
			if err := os.WriteFile(launcher, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			model := filepath.Join(dir, "model.gguf")
			if err := os.WriteFile(model, []byte("test model"), 0o600); err != nil {
				t.Fatal(err)
			}
			threadsFile := filepath.Join(dir, "threads")
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("LAYA_TEST_BINARY", binary)
			t.Setenv("LAYA_TEST_HELPER", "1")
			t.Setenv("LAYA_TEST_THREADS_FILE", threadsFile)
			t.Setenv("LLAMA_ARG_THREADS", tt.threads)
			if tt.stall {
				t.Setenv("LAYA_TEST_STALL", "1")
			} else {
				t.Setenv("LAYA_TEST_STALL", "")
			}

			addr := unusedAddress(t)
			var stdout bytes.Buffer
			var stderr lockedBuffer
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			args := []string{"--model", model, "--addr", addr, "--raw"}
			if tt.stall {
				args = append(args, "--timeout", "100ms")
			}
			args = append(args, `["report","other"]`, strings.Repeat("backup report ", 350))
			code := execute(ctx, args, environment(nil), &stdout, &stderr)
			if tt.stall && (code != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "deadline exceeded")) {
				t.Fatalf("exit code = %d, stdout = %q, stderr = %q", code, stdout.String(), stderr.String())
			}
			if !tt.stall && (code != 0 || stdout.String() != "report\n") {
				t.Fatalf("exit code = %d, stdout = %q, stderr = %q", code, stdout.String(), stderr.String())
			}
			threads, err := os.ReadFile(threadsFile)
			if err != nil {
				t.Fatal(err)
			}
			if string(threads) != tt.wantThreads {
				t.Fatalf("server thread setting = %q, want %q", threads, tt.wantThreads)
			}
			if conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond); err == nil {
				conn.Close()
				t.Fatal("one-shot server is still listening after the command exits")
			}
		})
	}
}

func TestExecuteReportsFailedServerStartup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the test server launcher uses a shell")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "llama-server"), []byte("#!/bin/sh\nexit 42\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	model := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(model, []byte("test model"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var stdout bytes.Buffer
	var stderr lockedBuffer
	code := execute(ctx,
		[]string{"--model", model, "--addr", unusedAddress(t), `["report","other"]`, "backup completed"},
		environment(nil), &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "llama-server exited before becoming ready: exit status 42") {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
}

func TestLlamaServerHelper(t *testing.T) {
	if os.Getenv("LAYA_TEST_HELPER") != "1" {
		return
	}

	flags := flag.NewFlagSet("llama-server", flag.ContinueOnError)
	flags.String("m", "", "")
	host := flags.String("host", "", "")
	port := flags.String("port", "", "")
	contextSize := flags.Int("c", 0, "")
	batchSize := flags.Int("b", 0, "")
	physicalBatch := flags.Int("ub", 0, "")
	parallel := flags.Int("parallel", 0, "")
	if err := flags.Parse(flag.Args()); err != nil {
		t.Fatal(err)
	}
	if *parallel < 1 {
		t.Fatal("invalid number of slots")
	}
	if err := os.WriteFile(os.Getenv("LAYA_TEST_THREADS_FILE"), []byte(os.Getenv("LLAMA_ARG_THREADS")), 0o600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(*host, *port))
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/v1/systemone", func(response http.ResponseWriter, request *http.Request) {
		var body systemone.Request
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		if os.Getenv("LAYA_TEST_STALL") == "1" {
			<-request.Context().Done()
			return
		}
		size := len(strings.Fields(body.State)) + 32
		if size > *batchSize || size > *physicalBatch || size > *contextSize / *parallel {
			http.Error(response, "input exceeds server batch or slot context", http.StatusBadRequest)
			return
		}
		_, _ = fmt.Fprint(response, `{"answers":{"decision":{"probabilities":{"report":0.9,"other":0.1}}}}`)
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
}

func TestRealLayaModel(t *testing.T) {
	model := os.Getenv("LAYA_INTEGRATION_MODEL")
	if model == "" {
		t.Skip("set LAYA_INTEGRATION_MODEL to run the real-model smoke test")
	}
	if _, err := exec.LookPath("llama-server"); err != nil {
		t.Fatalf("real-model smoke test requires llama-server on PATH: %v", err)
	}
	t.Setenv("LLAMA_ARG_THREADS", "")

	addr := unusedAddress(t)
	var stdout bytes.Buffer
	var stderr lockedBuffer
	code := execute(context.Background(),
		[]string{"--model", model, "--addr", addr, "--raw", `["joke","story","report"]`,
			strings.Repeat("This is a factual backup status report. ", 150)},
		environment(nil), &stdout, &stderr)
	if code != 0 || stdout.String() != "report\n" {
		t.Fatalf("exit code = %d, stdout = %q, stderr = %q", code, stdout.String(), stderr.String())
	}
	if conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond); err == nil {
		conn.Close()
		t.Fatal("one-shot server is still listening after the command exits")
	}
}

func TestParseEndpoint(t *testing.T) {
	baseURL, host, port, err := parseEndpoint("http://127.0.0.1:18080/")
	if err != nil {
		t.Fatal(err)
	}
	if baseURL != "http://127.0.0.1:18080" || host != "127.0.0.1" || port != "18080" {
		t.Fatalf("base=%q host=%q port=%q", baseURL, host, port)
	}
}

func TestParseEndpointRejectsPathAndMissingPort(t *testing.T) {
	for _, addr := range []string{"http://127.0.0.1:18080/api", "127.0.0.1"} {
		if _, _, _, err := parseEndpoint(addr); err == nil {
			t.Fatalf("parseEndpoint(%q) succeeded", addr)
		}
	}
}

func unusedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func decisionServer(t *testing.T, probabilities map[string]float64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/health":
			response.WriteHeader(http.StatusOK)
		case "/v1/systemone":
			response.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(response).Encode(map[string]any{
				"answers": map[string]any{
					"decision": map[string]any{
						"probabilities": probabilities,
					},
				},
			})
		default:
			http.NotFound(response, request)
		}
	}))
}

func environment(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
