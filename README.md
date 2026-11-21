# rsscollector

`rsscollector` is a small file-backed Go collector for RSS 2.0, RSS 1.0/RDF,
and Atom feeds. It preserves fetched feed bodies, normalizes entries, and keeps
immutable content revisions without requiring a database.

## Build and run

Go 1.25 or later is required.

```sh
go build -o rsscollector ./cmd/rsscollector
cp config.example.json config.json
./rsscollector validate --config config.json
./rsscollector run --config config.json
./rsscollector status --config config.json
```

Use `run --feed ID` to collect one configured feed and `run --force` to bypass
poll and retry schedules. The data directory is protected by an OS process
lock, and network requests have bounded time, redirects, and response size.
Private or local feed destinations are rejected unless
`http.allow_private_networks` is intentionally enabled.

Fetched bodies are retained in `data/feeds/<id>/snapshots/`; normalized entry
revisions and indexes are stored under `entries/`. State validators are committed
only after the snapshot has been parsed and persisted, allowing a pending
snapshot to be replayed safely.

## Configuration

See `config.example.json`. Feed IDs must be unique and contain only safe
filename characters. Unknown configuration fields are rejected. Runtime,
request, body-size, entry, and redirect limits are bounded.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Work completed or no feed was due |
| 1 | One or more collection or storage failures |
| 2 | Invalid configuration or command invocation |
| 3 | Another process holds the data-directory lock |
| 4 | The runtime budget expired |

## Test

```sh
go test ./...
```

This MVP does not scrape arbitrary web pages, infer deleted entries, deduplicate
across feeds, or run remote enrichment. Polling cannot recover entries that are
published and removed between successful runs.
