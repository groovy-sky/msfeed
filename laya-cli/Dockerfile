FROM alpine:3.21 AS cpp-builder

ARG LLAMA_CPP_REF=47d6192fce301f627c4d3e1c6910588c364dce0e

RUN apk add --no-cache build-base cmake git linux-headers
RUN git init /src/llama.cpp \
    && cd /src/llama.cpp \
    && git remote add origin https://github.com/ggml-org/llama.cpp.git \
    && git fetch --depth 1 origin "${LLAMA_CPP_REF}" \
    && git checkout --detach FETCH_HEAD \
    && cmake -S . -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=OFF \
        -DGGML_NATIVE=OFF \
        -DLLAMA_BUILD_EXAMPLES=OFF \
        -DLLAMA_BUILD_SERVER=ON \
        -DLLAMA_BUILD_TESTS=OFF \
        -DLLAMA_BUILD_UI=OFF \
        -DLLAMA_CURL=OFF \
        -DLLAMA_USE_PREBUILT_UI=OFF \
    && cmake --build build --target llama-server --parallel \
    && strip build/bin/llama-server

FROM golang:1.24-alpine AS go-builder

WORKDIR /src/laya-cli
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/laya \
    ./cmd/laya

FROM alpine:3.21

RUN apk add --no-cache libstdc++ libgomp \
    && addgroup -S -g 10001 laya \
    && adduser -S -D -H -u 10001 -G laya laya

COPY --from=cpp-builder /src/llama.cpp/build/bin/llama-server /usr/local/bin/llama-server
COPY --from=go-builder /out/laya /usr/local/bin/laya

USER 10001:10001
ENTRYPOINT ["/usr/local/bin/laya"]
