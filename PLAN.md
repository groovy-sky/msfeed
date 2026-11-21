# PLAN: Universal RSS Collector in Go

## 1. Objective

Build a compact, file-backed Go application that collects RSS and Atom feeds,
preserves original source material, tracks new and changed entries, and resumes
safely after failures.

The collector must work without SQL, Kubernetes, a queue broker, or an LLM.
Run it locally or through cron on a small machine.

“Universal” means provider-independent handling of RSS 2.0, RSS 1.0/RDF, and
Atom 1.0. It does not mean arbitrary website scraping or perfect support for
every malformed feed.

## 2. Scope

### Core
- Multiple configurable HTTP/HTTPS feeds.
- Feed parsing and normalization.
- Exact downloaded feed-body preservation.
- New-entry discovery and revision detection.
- Local, deterministic, versioned storage.
- Conditional HTTP fetching when supported.
- Persistent retry state and bounded execution.
- Structured logs and useful exit codes.

### Optional, later
- Provider-specific enrichment, including MCP.
- Article fetching.
- Summarization through an external service.
- Export to JSON Lines or Markdown.

### Not in scope
- Databases, distributed coordination, dashboards, or inbound webhooks.
- Local model inference.
- JavaScript rendering or automatic full-site crawling.
- Treating retrieved content as verified facts.

## 3. Operating Model

Use one executable and process feeds sequentially by default:

    configuration → fetch → preserve snapshot → parse → normalize
                  → reconcile entries → persist state → exit

A process lock prevents concurrent writers to the same data directory.
Release the lock automatically when the process exits; do not use the mere
existence of a lock file as proof that another process is running.

Each run has limits on duration, feed count, body size, entries processed, and
network requests. Stop scheduling new work when the runtime budget expires.

Core collection must remain independent of enrichment and summarization.

## 4. Configuration

Use one versioned JSON configuration file. JSON avoids an additional
configuration-format dependency.

Example:

```json
{
  "schema_version": 1,
  "data_dir": "./data",
  "max_run_seconds": 300,
  "http": {
    "timeout_seconds": 30,
    "max_body_bytes": 8388608,
    "max_redirects": 5,
    "user_agent": "rss-collector/1.0"
  },
  "defaults": {
    "poll_interval_seconds": 3600,
    "max_entries_per_run": 500
  },
  "feeds": [
    {
      "id": "azure-updates",
      "url": "https://www.microsoft.com/releasecommunications/api/v2/azure/rss",
      "enabled": true
    },
    {
      "id": "example",
      "url": "https://example.org/feed.xml",
      "enabled": true
    }
  ]
}
```

Requirements:
- Feed IDs are unique and restricted to safe filename characters.
- Feed IDs remain stable when URLs change.
- Reject unknown configuration fields and invalid limits.
- Optional authentication references environment variables or protected files;
  do not put secrets in logs or snapshots.
- Configuration overrides do not change existing record identities.
- URLs are configuration values, never hard-coded provider assumptions.

## 5. Repository Structure

```text
cmd/rsscollector/main.go
internal/
  config/
  runner/
  fetch/
  parse/
  normalize/
  identity/
  store/
  retry/
  output/
testdata/
  feeds/
  expected/
PLAN.md
README.md
go.mod
go.sum
```

Use the standard library where practical. Select and pin a maintained feed
parser after evaluating RSS, RDF, Atom, namespaces, character encodings, and
malformed-feed behavior. Do not implement a complete XML feed parser from
scratch merely to eliminate one dependency.

Keep interfaces small and focused:
- Fetcher: fetch one feed with conditional request metadata.
- Parser: convert a snapshot into normalized entries.
- Store: persist snapshots, entry revisions, and state.
- Clock: injectable time for deterministic tests.

Do not introduce a generic plugin framework for the first release.

## 6. Local Storage Layout

```text
data/
  collector.lock
  feeds/
    <feed-id>/
      feed.json
      state.json
      snapshots/
        <sha256>.xml
      snapshot-metadata/
        <sha256>.json
      entries/
        <entry-key>/
          index.json
          revisions/
            <content-hash>.json
  runs/
    <run-id>.json
```

- `feed.json`: configured identity and source URL; omit secrets.
- `state.json`: HTTP validators, last success, retry schedule, pending snapshot.
- Snapshots: immutable downloaded bodies; extension does not determine format.
- Snapshot metadata: retrieval time, effective URL, status, content type, hash.
- `index.json`: identity basis, first/last seen, current revision.
- Revisions: immutable normalized content and provenance.
- Run reports: counts, timings, failures, and budget exhaustion.

Reuse identical snapshot and revision hashes rather than rewriting them.
Observation timestamps belong in indexes/state, not in content hashes.

## 7. Normalized Entry Schema

```text
schema_version
entry_key
feed_id
identity_basis
source_id
title
url
authors[]
published_at
updated_at
raw_published_date
raw_updated_date
summary
content
content_type
categories[]
attachments[]
extensions
content_hash
provenance
```

Rules:
- Preserve original text and HTML; do not render HTML during collection.
- Normalize valid timestamps to UTC; retain original date strings.
- Missing or invalid dates remain unknown, not replaced with fetch time.
- Resolve relative links against the relevant document/base URL.
- Preserve useful namespaced extension fields without assuming a provider.
- Provenance records snapshot hash, retrieval time, and source URL.
- Do not invent source fields or silently discard parsing warnings.

The snapshot is the exact source artifact. A normalized entry is a derived
representation, not an exact reproduction of the original XML item.

## 8. Identity and Revisions

Identity is scoped to a feed; cross-feed deduplication is not automatic.

Identity precedence:
1. Nonempty Atom ID or RSS GUID, treated as an opaque identifier.
2. Entry link, with only conservative normalization.
3. Documented fallback fingerprint of available title, date, author, and content.

Compute:

    entry_key = SHA-256(feed_id + separator + identity_basis + separator + identity)

Do not remove query parameters or infer that different URLs are equivalent.
Record the identity basis so fallback decisions are auditable.

Fallback fingerprints cannot guarantee stable identity: edits may appear as
new entries. Report that limitation rather than attempting unsafe merges.

Compute revision hashes from a canonical serialization of normalized source
fields, excluding retrieval timestamps and incidental ordering where order
has no meaning.

Behavior:
- Same identity and content hash: update last-seen observation only.
- Same identity, new hash: append revision and update current pointer.
- Entry absent from a later feed: retain it; absence is not deletion.
- Never use publication time as the only deduplication mechanism.

## 9. Fetching and Retry Policy

- Use context cancellation and explicit timeouts.
- Support HTTP/HTTPS only; verify TLS normally.
- Persist ETag and Last-Modified when supplied.
- Issue conditional requests; handle 304 without reparsing.
- Bound decompressed response size, not just Content-Length.
- Limit redirects and validate each redirect target.
- Respect Retry-After where applicable.
- Retry transient transport failures, 429, and selected 5xx responses with
  exponential backoff, jitter, and a configured maximum.
- Persist next_retry_at; avoid long sleeping loops inside scheduled jobs.
- Treat most other 4xx responses as configuration/access errors.
- Persist malformed snapshots and report parse failure. If processing has not
  committed a batch yet, fetch a replacement on the next scheduled retry
  rather than replaying the same rejected bytes.

Important: do not activate new HTTP validators until the associated snapshot
has been successfully processed. Otherwise, a subsequent 304 could hide
unfinished work. Resume pending snapshots before fetching again.

Apply an explicit network policy: public destinations by default, with local
or private-network feeds available only through intentional configuration.
Never forward authentication across origins automatically.

## 10. Persistence and Recovery

For mutable files:
1. Write a temporary file in the destination directory.
2. Flush and sync the file.
3. Rename into place.
4. Sync the parent directory where supported.

Declare Linux as the initial crash-durability target; document filesystem and
platform limitations rather than claiming universal atomicity.

Commit order:
1. Persist snapshot.
2. Mark snapshot pending.
3. Persist entry revisions.
4. Update entry indexes.
5. Commit feed state and validators; clear pending marker.

File writes do not form a multi-file transaction. Recovery must replay the
pending snapshot idempotently and reconcile indexes with stored revisions.

Protect local directories with restrictive permissions. Do not delete corrupt
artifacts automatically: quarantine/report them and preserve recoverable data.

## 11. CLI and Scheduling

```text
rsscollector validate --config config.json
rsscollector run --config config.json
rsscollector run --config config.json --feed azure-updates
rsscollector run --config config.json --force
rsscollector status --config config.json
```

- `--force` bypasses poll/retry scheduling, not locks or safety limits.
- SIGINT/SIGTERM cancel requests and stop new work.
- A failed feed does not prevent processing other feeds.
- An unchanged feed does not prevent optional downstream work.

Exit codes:
- 0: selected due work completed, or nothing was due.
- 1: one or more collection/storage failures.
- 2: invalid configuration or invocation.
- 3: another process holds the lock.
- 4: runtime/work budget reached with unfinished selected work.

Cron should invoke the binary directly with absolute paths and log rotation.
The collector's configured poll intervals determine which feeds are due.

### Optional Git Branch Publication

For an auditable repository archive, `rsscollector archive` maps each stable
feed ID to an orphan branch named `rss/<feed-id>`. The collector remains the
single implementation of fetch, parse, retry, scheduling, and persistence
semantics; Git publication is an outer persistence step rather than a separate
scraper.

The archive command is optimized to avoid one checkout, process, or push per
feed:

1. Query all selected remote branch tips in one operation.
2. Fetch the existing shallow tips with one batched `git fetch`.
3. Materialize each branch through an isolated temporary Git index.
4. Run the feed with `data_dir` rooted at that temporary tree.
5. Stage only `feeds/<feed-id>/`; ephemeral locks and per-invocation reports
   are not committed.
6. Create a commit only when the resulting feed tree changed.
7. Push every changed branch in one atomic, force-with-lease-protected call.

Branch contents are constrained to `.gitignore` and that branch's feed data.
Symlinks, submodules, and unrelated paths are rejected instead of being
checked out and executed. A stable feed ID is preferred over encoding a URL in
the branch name: this avoids unsafe refs and preserves identity/state if the
configured URL changes. Duplicate source URLs in one archive run are rejected.

`.github/workflows/archive.yml` provides the scheduled deployment shape: one
runner, one checkout, one build, one invocation, and a repository-wide
concurrency group. GitHub Actions remains an optional transport; the same
command works locally with any Git remote.

Git branch publication does not remove repository-size constraints. It is
appropriate for small or medium archives where immutable history is the main
goal. Large bodies and high-volume feeds should use object storage and keep
branch contents limited to manifests and indexes.

## 12. Resource Discipline and Retention

- Default concurrency: one feed/request at a time.
- Keep memory bounded by the maximum feed size and parser behavior.
- Release snapshot/parser buffers before processing the next feed.
- No in-memory archive of all collected entries.
- Filter expensive downstream operations before invoking them.
- Keep structured logs small and rotate them externally.

Implement retention as an explicit later command, not automatic deletion.
Provide dry-run output; never delete snapshots referenced by retained revisions.
Warn on low disk space and fail safely rather than partially discarding history.

A finite RSS feed is not a historical archive. Polling cannot guarantee recovery
of entries published and removed between successful runs.

## 13. Testing and Acceptance Criteria

Test with local fixtures and HTTP test servers; no live service is required.

Coverage:
- RSS 2.0, RSS 1.0/RDF, Atom, namespaces, encodings, relative links.
- Missing GUIDs, invalid dates, duplicate items, reordered items.
- Unchanged entries and edited entries.
- ETag/Last-Modified, 304, redirects, 429, timeouts, oversized responses.
- Malformed XML and hostile payloads.
- Interrupted writes, pending replay, corrupt state, lock contention.
- Deterministic normalization and hash results.

Acceptance:
- A repeated fixture run creates no duplicate revision.
- A changed entry preserves both old and new revisions.
- Recovery converges to the same result as an uninterrupted run.
- Failure of one feed leaves other feeds usable.
- No credentials appear in emitted logs or stored metadata.
- Network, memory, and runtime limits are enforced.
- Collection works with all optional enrichment services unavailable.

Record actual peak memory and execution time using a representative fixture
set; choose operational limits from measurement, not unsupported guarantees.

Measured on 2026-11-21 for the six-fixture offline compatibility run:
0.557 seconds wall time and 155620 KiB peak resident memory, including the Go
test/toolchain process. This measurement is a development baseline, not a
production-memory guarantee.

## 14. Delivery Phases

### Phase 1 — Core collector
Configuration, fetching, parsing, snapshots, normalized entries, identity,
atomic persistence, CLI, lock, and fixture tests.

### Phase 2 — Reliable scheduled operation
Implement the six-target Microsoft compatibility suite in Section 16.
Conditional requests, durable retry state, pending recovery, resource budgets,
status command, run reports, failure-injection tests.

### Phase 3 — Optional downstream processing
Add a separate `enrich` command only if needed. Preserve separate task state,
source provenance, and retries. Provider-specific MCP and remote summarization
must not alter the universal collector's identity or storage semantics.

## 15. Definition of Done

One documented Go executable collects configured RSS/Atom feeds into an
auditable local directory, preserves revisions, survives interrupted execution,
and runs through cron without SQL, orchestration, or external inference.

## 16. Required Microsoft Feed Compatibility Suite

The following user-supplied URLs are required compatibility targets. Their
listing is a test requirement, not a claim that all endpoints are currently
available or return valid feeds.

| Feed ID | URL |
|---|---|
| microsoft-news | https://news.microsoft.com/source/feed/ |
| microsoft-blogs | https://blogs.microsoft.com/?feed=rss2 |
| microsoft-m365-updates | https://www.microsoft.com/releasecommunications/api/v2/m365/rss |
| microsoft-azure-updates | https://www.microsoft.com/releasecommunications/api/v2/azure/rss |
| microsoft-devblogs | https://devblogs.microsoft.com/feed |
| microsoft-msrc | https://api.msrc.microsoft.com/update-guide/rss |

### 16.1 Test Configuration and Assets

Create:

```text
configs/microsoft-feeds.json
testdata/feeds/microsoft/
  <feed-id>/
    original.xml
    capture.json
    expected.json
internal/integration/microsoft_feeds_test.go
```

The configuration includes all six targets with the exact supplied URLs.
Redirects are recorded; do not silently replace a configured URL with an
alternative endpoint.

`capture.json` records:
- Capture timestamp in UTC.
- Requested and effective URLs.
- HTTP status and redirect chain.
- Content-Type, Content-Encoding, ETag, and Last-Modified when present.
- SHA-256 of the stored response body.
- Capture tool version and parse warnings.

Do not commit credentials, cookies, or unrestricted full response headers.
Fixture captures must comply with applicable source terms; if redistribution
is unsuitable, retain captures locally and commit minimal representative
fixtures plus a reproducible capture procedure.

### 16.2 Two Separate Test Layers

**Offline regression suite — mandatory in normal CI**

Use reviewed snapshots from each reachable target. Tests must not depend on
live network access, current article titles, current item counts, or wall-clock
time. A synthetic fixture does not prove compatibility with a live endpoint.

For each fixture:
1. Parse without panic or silent loss of supported core fields.
2. Preserve title, links, IDs, dates, content, categories, and attachments when
   present; retain documented extension fields.
3. Preserve the original snapshot and associate normalized entries with it.
4. Confirm exact expected normalized fields and deterministic identity/hash.
5. Replay the same snapshot: create zero additional revisions.
6. Reorder entries: preserve existing identities.
7. Modify content while retaining identity: preserve the old revision and
   create one new revision.
8. Simulate interruption and confirm idempotent replay.

Select edge cases from actual captured data rather than assuming every feed
contains GUIDs, full content, attachments, or particular namespaces.

**Live compatibility suite — opt-in**

Example invocation:

```sh
RSSCOLLECTOR_LIVE_TESTS=1 go test ./internal/integration -run TestMicrosoftFeeds -v
```

Live tests run only when explicitly enabled and use sequential requests, the
configured user agent, timeouts, body limits, redirect limits, and retry policy.
They must not execute during normal offline CI.

Separate parser defects from transient outages, access restrictions, and
configuration errors. In an explicitly requested live run, unreachable targets
must be reported as unsuccessful or unverified, not silently skipped as passed.

### 16.3 Live Checks Per URL

- Record transport, TLS, status, and redirect results.
- For a successful response, check that the body is a supported feed rather
  than an HTML error page, login screen, or bot challenge.
- Treat unexpected Content-Type as a warning if the body parses as a feed;
  never accept HTML solely because the status is 200.
- Parse the body and report normalized entry count and warnings.
- Allow a valid empty feed, but mark entry-level checks as not exercised.
- Check optional IDs, dates, links, HTML, categories, and extension handling
  against the fields actually present.
- Persist and replay the captured snapshot to test idempotency.
- If validators are supplied, make one conditional follow-up request:
  accept 304, or parse a 200 response as possibly changed content.
- If validators are absent, report conditional-fetch coverage as unavailable,
  not failed.
- Never require a specific title, ID, publication date, feed format, or item
  count without evidence from the captured fixture.

Use local HTTP test servers, not repeated calls to Microsoft, for simulated
429/5xx responses, malformed bodies, timeout behavior, and redirect attacks.

### 16.4 Results and Release Gate

Write a machine-readable and human-readable report containing:

```text
feed_id
requested_url
effective_url
checked_at
transport_result
http_status
parse_result
detected_format
entry_count
normalization_result
idempotency_result
conditional_fetch_result
warnings[]
failure_class
```

Check categories: PASS, FAIL, NOT_EXERCISED, UNVERIFIED.

A target receives an overall PASS only after successful live fetching, parsing,
persistence, and replay. Optional unsupported features do not prevent PASS,
but their coverage limitations must be visible.

Before claiming support for all six feeds:
- Obtain and review a real capture from each reachable endpoint.
- Pass offline regression tests for all six captured targets.
- Complete a live compatibility run and retain the timestamped report.
- Document unresolved endpoint/access issues rather than claiming success.
- Ensure all feed-specific behavior remains in fixtures or optional adapters;
  core parsing, identity, and persistence stay provider-independent.

### 16.5 Verification Status at Plan Update

The opt-in Go live suite ran at 2026-11-21T15:31Z. It verified the
`microsoft-m365-updates` and `microsoft-azure-updates` endpoints by fetching,
parsing, persisting, and replaying their snapshots, including all entry batches
for the 2,401-entry M365 response. Neither endpoint supplied a validator during
the check, so conditional-fetch coverage remains `NOT_EXERCISED`.

The other four configured hostnames failed DNS resolution in the execution
environment and remain `UNVERIFIED`. This transport result is not evidence that
the endpoints are unavailable generally. Sanitized machine-readable and
human-readable results are retained in `testdata/reports/`.

Only two targets therefore have live compatibility evidence. Do not infer that
all six are supported or replace configured URLs based on their names. Current
article bodies are not committed; offline CI uses clearly marked representative
fixtures, and the capture procedure retains real bodies only in an explicitly
selected local directory.
