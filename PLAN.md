Implementation Plan: laya CLI Decision Tool (Go + ggml-org/Laya-GGUF)

1. Objective & CLI Contract

Build a self-contained, minimal-footprint CLI tool (laya) written in Go that wraps ggml-org/Laya-GGUF (via llama.cpp's /v1/systemone inference engine) to perform fast, single-command choice classification.

Target CLI Interface

./laya '["joke","story","report"]' "26.11.2026 backup completed successfully"


Argument 1 (choices): JSON array of candidate labels (["joke","story","report"]) or a JSON object mapping labels to rich descriptions ({"joke":"A humorous anecdote","report":"A factual status log"}).

Argument 2 (state): Input text/context to classify.

Optional Flags:

--model (env: LAYA_MODEL, default: /models/laya.gguf): Path to the quantized GGUF model file.

--addr (env: LAYA_ADDR, default: 127.0.0.1:18080): Address of an existing llama-server daemon or ephemeral local bind address.

--raw (bool, default: false): Print only the winning label string (e.g., report) for shell piping; otherwise print structured JSON (choice + probabilities).

--min-confidence (float, default: 0.0): Optional confidence threshold; exits with non-zero status (2) if top probability is below threshold.

2. Architecture Overview

Because ggml-org/Laya-GGUF uses a ModernBERT-large encoder with a specialized decision head exposed in llama-server (/v1/systemone), the Go binary (laya) acts as a zero-dependency static client + lifecycle supervisor:

Daemon Check (Warm Path, < 15 ms): laya sends a fast GET /health check to --addr. If a warm llama-server instance is already listening, laya immediately posts to /v1/systemone.

Ephemeral Server Spawn (Cold Path, ~200 ms): If no server is listening on --addr, laya spawns llama-server as a child subprocess (exec.CommandContext), polls /health until ready, runs the /v1/systemone decision query, and cleanly terminates the subprocess on exit.

Structured Decision Parsing: Parses /v1/systemone response probabilities, ranks candidates, validates optional confidence thresholds, and prints clean output to stdout.

3. Repository Structure

laya-cli/
├── cmd/
│   └── laya/
│       └── main.go          # CLI entrypoint, flag parsing, server lifecycle & HTTP client
├── internal/
│   └── systemone/
│       ├── client.go        # Typed /v1/systemone request/response structs & HTTP client
│       └── client_test.go   # Unit tests with httptest mock server
├── go.mod
├── Dockerfile               # 3-stage minimal build (C++ llama-server + static Go binary + scratch/alpine)
└── PLAN.md


4. Implementation Milestones

Phase 1: Go Client & /v1/systemone Protocol (internal/systemone)

Define Request/Response Models:

SystemOneRequest: { State string, Questions map[string]ChoiceQuestion }

ChoiceQuestion: { Type: "choice", Options map[string]string }

SystemOneResponse: { Answers map[string]ChoiceAnswer } where ChoiceAnswer holds Probabilities map[string]float64.

Input Normalization Helper:

Unmarshal Argument 1 first as []string (mapping each element k -> k).

Fallback to unmarshaling as map[string]string to support rich option descriptions without changing CLI ergonomics.

Decision Ranking & Gating:

Sort probabilities descending to determine TopChoice, TopProb, and Margin (top1 - top2).

Phase 2: CLI & Subprocess Lifecycle (cmd/laya/main.go)

Flag & Env Configuration: Use standard library flag + os.Getenv (zero external Go dependencies to keep binary small and auditable).

Process Management:

Probe http://<addr>/health with a 100ms timeout.

On connection refused, start llama-server -m <model> --host <host> --port <port> -c 2048 -b 512 -ub 512 --parallel 2.

Poll /health every 25ms up to 10s.

Register defer + signal.NotifyContext (SIGINT, SIGTERM) to guarantee child llama-server cleanup.

Output Formatting:

Default JSON mode: {"choice":"report","probabilities":{"joke":0.004,"story":0.009,"report":0.987}}

--raw mode: report

Phase 3: Model Preparation (ggml-org/Laya-GGUF)

Download Laya-Q8_0.gguf (~450 MB, recommended balance of size and accuracy) or Laya-BF16.gguf (~845 MB) from https://huggingface.co/ggml-org/Laya-GGUF.

Verify decision accuracy across representative prompts (joke, story, report, and edge cases with a fallback "other" label).

Phase 4: Minimal Multi-Stage Docker Image

Build a 3-stage Dockerfile combining a stripped static llama-server, a static CGO_ENABLED=0 Go laya binary, and a minimal Alpine runtime (~30 MB image size excluding mounted model):

Stage 1 (cpp-builder on alpine:3.21):

Compile llama-server with -DBUILD_SHARED_LIBS=OFF -DLLAMA_CURL=OFF -DLLAMA_BUILD_TESTS=OFF -DLLAMA_BUILD_EXAMPLES=OFF.

Run strip build/bin/llama-server.

Stage 2 (go-builder on golang:1.24-alpine):

Build CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -trimpath -o /bin/laya ./cmd/laya.

Stage 3 (runtime on alpine:3.21):

Install only libstdc++ and libgomp (required by llama-server OpenMP runtime; no curl, jq, or shell wrappers needed).

Copy /usr/local/bin/llama-server and /usr/local/bin/laya.

Run as non-root user (uid 10001) with ENTRYPOINT ["/usr/local/bin/laya"].

5. Verification & Usage Checklist

[ ] Unit Tests: go test ./... passes against mock /v1/systemone payloads (array choices, object descriptions, confidence threshold failures).

[ ] Image Size Check: docker images laya-cli:minimal confirms base image footprint is ~30–35 MB (without baked model).

[ ] Cold One-Shot Run:

docker run --rm -v "$(pwd)/models:/models:ro" laya-cli:minimal \
  '["joke","story","report"]' \
  "26.11.2026 backup completed successfully"


[ ] Warm Daemon Mode (High Throughput):

# Start persistent background server
docker run -d --name laya-daemon -p 18080:18080 \
  -v "$(pwd)/models:/models:ro" \
  --entrypoint /usr/local/bin/llama-server \
  laya-cli:minimal -m /models/laya.gguf --host 0.0.0.0 --port 18080 -c 2048 -b 512 -ub 512 --parallel 4

# Execute sub-15ms CLI calls against the warm daemon
LAYA_ADDR=127.0.0.1:18080 ./laya --raw '["joke","story","report"]' "26.11.2026 backup completed successfully"
