# rsscollector

`rsscollector` is a small file-backed Go collector for RSS 2.0, RSS 1.0/RDF,
and Atom feeds. It preserves fetched feed bodies, normalizes entries, and keeps
immutable content revisions without requiring a database.

Identity is scoped to a feed: RSS GUID/Atom ID is preferred, then a
conservatively resolved entry link, then a documented fingerprint of available
title, dates, authors, summary, and content. The fallback is not stable when an
entry is edited, so such edits may appear as new entries rather than revisions.

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
snapshot to be replayed safely. Pending snapshots larger than the configured
entry budget are processed in deterministic batches across runs without
refetching.

If a response cannot be decoded or parsed before any entry batch commits, the
collector keeps the original bytes and records the snapshot hash in the run
report. The next scheduled retry fetches a replacement using only the last
good HTTP validators. Local storage corruption leaves pending work in place
for inspection rather than treating it as a bad response from the publisher.

## Configuration

See `config.example.json`. Feed IDs must be unique and contain only safe
filename characters. Unknown configuration fields are rejected. Runtime,
request, body-size, entry, and redirect limits are bounded.

Optional request authentication is configured per feed without storing the
secret in `feed.json`:

```json
"authentication": {
  "header_name": "Authorization",
  "value_env": "RSS_FEED_AUTH"
}
```

Use `value_file` instead of `value_env` when appropriate; relative paths are
resolved from the configuration file. Secret files must be regular files with
owner-only permissions. Authentication headers are removed on cross-origin
redirects.

Retry backoff is persisted as `next_retry_at`; scheduled invocations do not
sleep. `Retry-After` is honored, and transient `429` and selected `5xx` responses
use bounded exponential backoff with configured jitter.

The Microsoft configuration enables `"public_dns_fallback": true` under
`http`. For direct connections, the collector tries system DNS first. If
that lookup fails, it tries Cloudflare, Quad9, Google, and OpenDNS in that
order, with one second per public resolver within the fetch timeout. These
queries send the feed or redirect hostname (not the URL or credentials) to
external DNS providers. Disable the option if your network forbids that.
Fallback does not retry HTTP denials or connection failures and does not
change TLS verification or the public-destination policy.

## Microsoft compatibility suite

`configs/microsoft-feeds.json` contains the six Microsoft targets from
`PLAN.md`. Normal tests use reviewed, synthetic representative fixtures in
`testdata/feeds/microsoft/` so CI never depends on the network or redistributes
current article content. Their capture records explicitly remain `UNVERIFIED`
until the endpoints are checked live.

Run the opt-in live suite sequentially with:

```sh
RSSCOLLECTOR_LIVE_TESTS=1 \
RSSCOLLECTOR_REPORT_DIR="$PWD/reports" \
go test ./internal/integration -run TestMicrosoftFeeds -v
```

The live suite writes sanitized machine-readable JSON and human-readable text
reports. It reports access restrictions, outages, parser failures, and
unavailable conditional-fetch coverage separately; it does not silently turn an
unverified endpoint into a pass. Set `RSSCOLLECTOR_LIVE_CAPTURE_DIR` to a
protected local directory to retain exact response bodies and their capture
metadata for review. Raw captures are not written to the repository by default.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Work completed or no feed was due |
| 1 | One or more collection or storage failures |
| 2 | Invalid configuration or command invocation |
| 3 | Another process holds the data-directory lock |
| 4 | Runtime, work, feed, or request budget expired with unfinished work |

## Test

```sh
go test ./...
```

This MVP does not scrape arbitrary web pages, infer deleted entries, deduplicate
across feeds, or run remote enrichment. Polling cannot recover entries that are
published and removed between successful runs. Crash durability targets Linux
filesystems; other platforms and filesystems require their own durability
validation. Retention remains an explicit future command—there is no automatic
deletion of snapshots or revisions.
