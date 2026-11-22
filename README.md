# rsscollector

`rsscollector` is a small file-backed Go collector for RSS 2.0, RSS 1.0/RDF,
and Atom feeds. It preserves fetched feed bodies and keeps compact, immutable
entry records without requiring a database.

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
./rsscollector archive --config config.json --repository .
```

Use `run --feed ID` to collect one configured feed and `run --force` to bypass
poll and retry schedules. The data directory is protected by an OS process
lock, and network requests have bounded time, redirects, and response size.
Private or local feed destinations are rejected unless
`http.allow_private_networks` is intentionally enabled.

Fetched bodies are retained in `data/feeds/<id>/snapshots/`; compact entry
revisions and indexes are stored under `entries/`. A revision can contain:

```json
{
  "url": "https://example.com/original-update",
  "title": "Original update title",
  "published_at": 1794499202,
  "summary": "The description supplied by the feed.",
  "content": "The full article text supplied by the feed, when different from the summary."
}
```

`published_at` is a Unix timestamp in seconds, using the updated date when no
valid published date exists, or `0` when the feed has neither. Text in `summary`
and `content` is converted to plain text without truncation. If the feed only
provides article text, it goes in `summary`; `content` is only written when it
adds distinct text. The collector does not fetch linked articles when a feed
only supplies an excerpt. Original HTML, provider extensions, and provenance
remain in the snapshot and snapshot-metadata directories.

Feeds may optionally declare an OData content source to recover text that the
RSS payload truncates. Provider text is cleaned to plain text and stored
separately at `entries/<entry-key>/enrichments/<content-hash>.json`, with the
current artifact selected by `enrichment.json`. Each artifact records its
provider, source and article URLs, RSS entry ID, entry revision, retrieval
time, and text. This keeps provider failures and content changes independent
from the feed's identity and compact revisions.

State validators are committed only after the snapshot has been parsed and
persisted, allowing a pending snapshot to be replayed safely. Pending snapshots
larger than the configured entry budget are processed in deterministic batches
across runs without refetching. Existing full entry revisions are compacted
automatically on the next run without cropping their text. When upgrading from
the cropped 320-character format, the latest successfully processed snapshot
is replayed before any network fetch; previously cropped files remain intact
and a new uncropped revision becomes current. Missing provider artifacts are
retried even when the next feed request returns `304 Not Modified`.

If a response cannot be decoded or parsed before any entry batch commits, the
collector keeps the original bytes and records the snapshot hash in the run
report. The next scheduled retry fetches a replacement using only the last
good HTTP validators. Local storage corruption leaves pending work in place
for inspection rather than treating it as a bad response from the publisher.

## Git branch archive

The `archive` command publishes one configured feed to one orphan Git branch,
using stable branches named `rss/<feed-id>` by default:

```sh
./rsscollector archive \
  --config configs/microsoft-feeds.json \
  --repository . \
  --remote origin
```

Feed branches contain only that feed's `feeds/<id>/` data. `collector.lock`
and one-off run reports are ignored, because they add Git churn without
representing collection state. The branch's `feed.json` records the configured
URL, so a URL change can be made without changing the branch or entry identity.
The command rejects duplicate configured URLs and refuses to process a branch
that contains another feed or unrelated or symlinked paths.

The Microsoft configuration publishes these isolated archives:

| Branch | RSS URL |
| --- | --- |
| `rss/microsoft-news` | https://news.microsoft.com/source/feed/ |
| `rss/microsoft-blogs` | https://blogs.microsoft.com/?feed=rss2 |
| `rss/microsoft-m365-updates` | https://www.microsoft.com/releasecommunications/api/v2/m365/rss |
| `rss/microsoft-azure-updates` | https://www.microsoft.com/releasecommunications/api/v2/azure/rss |
| `rss/microsoft-devblogs` | https://devblogs.microsoft.com/feed |
| `rss/microsoft-msrc` | https://api.msrc.microsoft.com/update-guide/rss |

All six archives retain complete text supplied by their feeds. The Azure
archive also enriches source-truncated descriptions from Microsoft's
Release Communications JSON API; no arbitrary linked page is fetched.

The archive path is deliberately batched:

1. Resolve all enabled feed branches with one remote query.
2. Fetch existing branch tips together with shallow refspecs.
3. Materialize and collect each branch sequentially using isolated temporary
   Git indexes, so no worktree checkout or second repository clone is needed.
4. Commit only changed feed state, snapshots, and revisions.
5. Push all changed branches in one atomic, lease-protected operation.

This keeps Git transport and runner startup overhead independent of the number
of feeds. Conditional requests, pending snapshots, retry state, runtime,
request, feed, and entry budgets remain part of the collector rather than
being reimplemented by the workflow. A run may be inspected without publishing
by adding `--dry-run`; `--feed ID` selects one branch and `--force` bypasses
the normal poll and retry schedules.

`.github/workflows/archive.yml` runs this mode daily at 03:17 UTC with one
checkout and one Go build. It can also be started manually, optionally for one
feed. A workflow concurrency group prevents two archive runs from writing the
same branches concurrently. Ensure Actions has write permission for the
repository before enabling scheduled publishing. Authentication values for
private feeds must be supplied through Actions secrets; never place the secret
itself in the configuration or feed branch.

Branch-per-feed publication is suitable for small to medium auditable archives.
GitHub and Git have repository, object, and file-size limits, and every
immutable snapshot or revision contributes to branch history. There is no
automatic retention yet; high-volume use may require object storage with Git
holding only indexes and manifests.

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

Optional OData enrichment is configured per feed:

```json
"enrichment": {
  "provider": "odata",
  "url": "https://example.org/api/updates",
  "id_field": "id",
  "content_field": "description",
  "content_type": "text/html",
  "max_items_per_request": 100
}
```

The RSS GUID/Atom ID is matched to `id_field`; the provider's `content_field`
is converted to plain text while dropping script and style content. Requests
use bounded OData batches and share the feed HTTP timeout, body, redirect,
network, and total request budgets. Remote provider failures are reported as
warnings and never prevent the underlying RSS record from being committed;
local artifact corruption or write failures remain explicit storage failures.
The Microsoft Azure feed uses this mode because its RSS descriptions are
source-truncated while the public Release Communications JSON API supplies
the complete description. Linked web pages are not scraped.

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

This MVP does not scrape arbitrary web pages, render JavaScript, infer deleted
entries, or deduplicate across feeds. Provider enrichment is limited to
configured OData endpoints. Polling cannot recover entries that are published
and removed between successful runs. Crash durability targets Linux
filesystems; other platforms and filesystems require their own durability
validation. Retention remains an explicit future command—there is no automatic
deletion of snapshots, revisions, or enrichment artifacts.
