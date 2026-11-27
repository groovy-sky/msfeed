# laya CLI

`laya` is a standalone Go client for llama.cpp's `/v1/systemone` decision
endpoint. It does not become part of the `rsscollector` executable. The model
is mounted at runtime rather than stored in the image.

## Build

Build the binary:

```sh
cd laya-cli
CGO_ENABLED=0 go build -o laya ./cmd/laya
```

Build the separate image:

```sh
cd laya-cli
docker build -t laya-cli:minimal .
```

The Docker build accepts `--build-arg LLAMA_CPP_REF=<branch-or-commit>` for
pinning the llama.cpp source revision.

`.github/workflows/laya-image.yml` publishes the same multi-stage image to
`ghcr.io/<owner>/rsscollector-laya` when `laya-cli/` changes on `main`. It
publishes `latest` plus a commit-SHA tag, while the archive workflow pulls the
published image instead of compiling llama.cpp for every collection.

The default amd64 build may require AVX2, FMA, F16C, BMI2, and SSE4.2 CPU
instructions; run it on a compatible host.

## Model

Download the quantized model separately and name it `laya.gguf`:

```sh
mkdir -p models
curl --fail --location \
  -o models/laya.gguf \
  https://huggingface.co/ggml-org/Laya-GGUF/resolve/main/Laya-Q8_0.gguf
```

The mounted file must be readable by the image's UID 10001. A different model
path can be supplied with `--model` or `LAYA_MODEL`.

## Use

Choices can be a JSON string array:

```sh
./laya \
  '["joke","story","report"]' \
  "26.11.2026 backup completed successfully"
```

Or an object whose values provide richer descriptions:

```sh
./laya \
  '{"joke":"A humorous anecdote","report":"A factual status log"}' \
  "26.11.2026 backup completed successfully"
```

The default output is JSON:

```json
{"choice":"report","probabilities":{"joke":0.004,"story":0.009,"report":0.987}}
```

Use `--raw` to print only the label and `--min-confidence` to return exit code
2 when the winning probability is too low. `--model`/`LAYA_MODEL` defaults to
`/models/laya.gguf`; `--addr`/`LAYA_ADDR` defaults to `127.0.0.1:18080`.
`--timeout` controls the decision request timeout (default `5m`).

When the connection is refused, the CLI starts `llama-server`, waits up to 30
seconds for `/health`, performs the request, and terminates the child before
exiting. An existing server reporting that it is loading is also given time to
become ready. Put `llama-server` on `PATH` when using the binary directly. The
image already contains it.

The one-shot server defaults to one inference thread so it also works with
container CPU quotas. Set `LLAMA_ARG_THREADS` to use more threads when the
deployment has sufficient CPU capacity. The one-shot context and batch are
2048 tokens, including the question and choices. Larger inputs require an
externally managed server configured with a larger context and batch.

One-shot container use:

```sh
docker run --rm \
  -v "$(pwd)/models:/models:ro" \
  laya-cli:minimal \
  '["joke","story","report"]' \
  "26.11.2026 backup completed successfully"
```

Warm daemon use:

```sh
docker run -d --name laya-daemon -p 18080:18080 \
  -v "$(pwd)/models:/models:ro" \
  -e LLAMA_ARG_THREADS=1 \
  --entrypoint /usr/local/bin/llama-server \
  laya-cli:minimal \
  -m /models/laya.gguf --host 0.0.0.0 --port 18080 \
  -c 8192 -b 2048 -ub 2048 --parallel 4

LAYA_ADDR=127.0.0.1:18080 ./laya --raw \
  '["joke","story","report"]' \
  "26.11.2026 backup completed successfully"
```

The daemon's context per slot is its `-c` value divided by `--parallel`; its
physical batch (`-ub`) must also fit the complete request.

`rsscollector archive --laya /path/to/laya` uses this executable to assign the
fixed states `Generally Available`, `Preview`, `Retirement`, or `Other` to
stored entries. The daily archive workflow starts one warm daemon and reuses
it for all entries, so the Q8 model is loaded only once per job.

## Tests

```sh
go test ./...
LAYA_INTEGRATION_MODEL="$PWD/models/laya.gguf" go test ./cmd/laya -run TestRealLayaModel -v
```

The optional real-model test requires `llama-server` on `PATH` and exercises
an article-sized input through a managed one-shot server.
