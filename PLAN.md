# Pi Token Smith v1: Capture Complete Observable Model Billing Evidence

Pi Token Smith v1 is a passive, fail-open Pi extension backed by a CGO-free Go daemon. It captures and preserves everything Pi can locally observe about model-billed exchanges, stores the evidence in SQLite with FTS5, and exposes exact retrieval through CLI, HTTP, and MCP. Analysis and optimization recommendations are intentionally deferred until real evidence exists.

## v1 outcome

An installed Pi extension continuously captures observable provider requests, finalized responses, reported usage, nested model usage, and relevant lifecycle events without modifying or blocking Pi. A single Go daemon owns the global database and serves every access interface.

## Non-negotiable constraints

| Constraint | Decision |
|---|---|
| Host behavior | Passive observation only; never mutate Pi prompts, context, tools, requests, responses, or control flow |
| Failure policy | Fail open; Pi continues if capture, transport, daemon, or storage fails |
| Analysis | No reports, recommendations, secret detection, or optimization logic in v1 |
| Fidelity | Preserve complete locally observable evidence without masking, normalization, or summarization |
| Usage authority | Provider-reported usage and cost are authoritative |
| Scope | One global store with every record identified by project |
| Retention | Unlimited until explicit manual deletion |
| Encryption | No encryption at rest in v1 |
| Runtime isolation | Go daemon owns persistence; the TypeScript extension remains thin |
| SQLite | CGO-free `modernc.org/sqlite` with FTS5 |
| Network exposure | Automatic capture uses a Unix socket; HTTP is explicit and loopback-only |

## System architecture

```text
Pi lifecycle and provider events
             │
             ▼
Thin Pi extension (TypeScript)
             │ versioned Unix RPC
             ▼
Pi Token Smith daemon (Go)
             │
             ▼
SQLite + FTS5
     ▲        ▲        ▲
     │        │        │
    CLI   HTTP API   MCP stdio
```

### TypeScript extension

The extension is responsible only for:

- Observing Pi lifecycle, provider, message, and tool events.
- Assigning correlation IDs and per-session sequence numbers.
- Taking snapshots of locally observable evidence.
- Sending capture envelopes to the daemon.
- Ensuring the daemon is running at session startup.
- Remaining fail-open when any Token Smith component fails.

It must not:

- Change or replace event payloads.
- Inject messages or instructions.
- block provider requests.
- Analyze captured content.
- Open SQLite directly.

### Go daemon

The daemon is the sole database owner. It is responsible for:

- Single-instance locking and stale socket recovery.
- Unix socket RPC.
- Protocol validation and idempotent event ingestion.
- Schema migrations.
- SQLite WAL configuration and transactions.
- FTS5 projection maintenance.
- Query and raw evidence retrieval services.
- Recovery of incomplete exchanges.
- Health and diagnostic information.

CLI, HTTP, and MCP communicate with the daemon. They do not open SQLite independently.

## Runtime locations and permissions

```text
~/.pi/agent/pi-token-smith/
├── token-smith.sqlite
├── token-smith.sock
├── token-smith.lock
└── http.token
```

| Resource | Required mode |
|---|---:|
| Runtime directory | `0700` |
| SQLite database | `0600` |
| Unix socket | `0600` |
| HTTP token | `0600` |

The database lives outside repositories. No captured evidence is written into project directories.

## Process lifecycle

### Extension startup

1. Resolve the canonical project path and stable project ID.
2. Probe the Unix socket.
3. Reuse a healthy daemon when available.
4. Attempt one bounded daemon start when unavailable.
5. Continue without capture if startup fails.
6. Open or resume the Pi session in the daemon.

### Daemon startup

1. Create and validate the protected runtime directory.
2. Acquire the single-instance lock.
3. Recover a stale socket only when no live daemon owns it.
4. Open SQLite.
5. Apply transactional migrations.
6. enable and verify foreign keys, WAL, and busy timeout.
7. Verify FTS5 with a real virtual-table probe.
8. Run `PRAGMA quick_check`.
9. Mark abandoned pending exchanges as `capture_incomplete`.
10. Start accepting Unix socket requests.

### Shutdown

- Pi session shutdown records a session event but does not stop the shared daemon.
- The daemon handles termination signals and closes SQLite cleanly.
- Shutdown never performs destructive repair.

## Capture model

### Correlation hierarchy

```text
project
└── session
    └── agent run
        └── turn
            ├── provider exchange
            ├── tool calls
            └── tool results
```

Every captured event contains:

- Stable event UUID.
- Protocol version.
- Project ID.
- Session ID.
- Optional agent-run, turn, and exchange IDs.
- Monotonic per-session sequence number.
- Event occurrence timestamp.
- Daemon receipt timestamp.
- Payload byte length.
- SHA-256 of stored payload bytes.

Chronology remains authoritative even when higher-level correlation is incomplete.

### Pi event mapping

| Pi event | Evidence |
|---|---|
| `session_start` | Project, session, file, and startup reason |
| `before_agent_start` | User prompt, images, and observable system prompt |
| `turn_start` | Turn index and timestamp |
| `before_provider_request` | Complete provider payload observable at Token Smith's extension position |
| `after_provider_response` | Available HTTP status and response headers |
| `message_end` | Finalized user, assistant, and tool-result messages |
| `tool_call` | Tool name, call ID, and complete arguments |
| `tool_result` | Complete result, error state, and nested usage |
| `turn_end` | Final turn message and associated tool results |
| `session_compact` | Compaction evidence and tokens before compaction |
| `agent_end` | Final agent-run messages |
| `session_shutdown` | Session close or replacement reason |

### Evidence terminology

Token Smith must identify evidence by what Pi actually exposes:

- `pi_provider_payload_json`
- `pi_assistant_message_json`
- `pi_tool_result_json`
- `provider_reported_usage`
- `provider_response_metadata`

It must not label provider payload objects as exact HTTP request bytes. Extensions loaded after Token Smith may still mutate a provider payload, so the observation position must remain explicit.

### Usage sources

Usage records distinguish:

```text
provider_exchange
nested_tool
compaction
branch_summary
```

This distinction prevents accidental double counting and supports later reconciliation against Pi session totals.

## Internal RPC protocol

### Transport

- Unix domain socket at `token-smith.sock`.
- Persistent reusable connections.
- Eight-byte unsigned big-endian frame length.
- JSON frame body.
- Multiple framed messages per connection.

### Request envelope

```json
{
  "protocol_version": 1,
  "request_id": "uuid",
  "operation": "capture.append",
  "sent_at": "2026-04-13T00:00:00Z",
  "body": {}
}
```

### Response envelope

```json
{
  "protocol_version": 1,
  "request_id": "uuid",
  "status": "ok",
  "body": {}
}
```

Errors use stable machine-readable codes. Unknown operations and incompatible protocol versions fail explicitly. Unknown fields may be ignored only within a compatible protocol version.

### Initial operations

```text
system.hello
system.health
system.info
capture.append
capture.append_batch
capture.stats
project.list
project.get
session.list
session.get
exchange.list
exchange.get
event.get
event.read_payload
search.events
```

Capture is idempotent by event ID. The daemon returns acknowledgements, but Pi capture does not wait synchronously for durable persistence.

## SQLite storage

### Source of truth

`capture_events` is immutable and append-only.

```sql
CREATE TABLE capture_events (
    id                TEXT PRIMARY KEY,
    protocol_version  INTEGER NOT NULL,
    event_type        TEXT NOT NULL,
    project_id        TEXT NOT NULL,
    session_id        TEXT NOT NULL,
    exchange_id       TEXT,
    sequence          INTEGER NOT NULL,
    occurred_at       TEXT NOT NULL,
    received_at       TEXT NOT NULL,
    payload           BLOB NOT NULL,
    payload_encoding  TEXT NOT NULL,
    payload_size      INTEGER NOT NULL,
    payload_sha256    TEXT NOT NULL
);
```

Events are never overwritten. Stored payload bytes, byte length, and SHA-256 provide integrity evidence.

### Query projections

Derived relational projections include:

- `projects`
- `sessions`
- `agent_runs`
- `turns`
- `exchanges`
- `provider_attempts`
- `usage_records`

A single ingestion transaction must:

1. Insert the immutable capture event.
2. Update the relevant relational projection.
3. Update the FTS5 projection.
4. Commit all changes together.

Any failure rolls back the complete ingestion transaction.

### FTS5

FTS5 is derived and rebuildable. It is never audit evidence.

```sql
CREATE VIRTUAL TABLE capture_fts USING fts5(
    event_id UNINDEXED,
    project_id UNINDEXED,
    session_id UNINDEXED,
    exchange_id UNINDEXED,
    content
);
```

The original payload remains in `capture_events`. Search returns event references; callers explicitly retrieve original evidence afterward.

### Driver verification

Use `modernc.org/sqlite`, following the proven CGO-free pattern used by Engram. Pin compatible `modernc.org/sqlite` and `modernc.org/libc` versions.

Startup and tests must prove FTS5 availability by creating an FTS5 probe table and executing an insert plus `MATCH` query. Missing FTS5 is a storage startup failure; it must not silently degrade to another search implementation.

## Access interfaces

### CLI

Initial commands:

```bash
pi-token-smith daemon
pi-token-smith status
pi-token-smith doctor
pi-token-smith projects
pi-token-smith sessions
pi-token-smith search <query>
pi-token-smith inspect <exchange-id>
pi-token-smith serve
pi-token-smith mcp
```

CLI commands query the daemon through RPC.

### HTTP API

- Disabled by default.
- Started explicitly with `pi-token-smith serve`.
- Binds to `127.0.0.1` by default.
- Requires a Bearer token.
- Disables CORS by default.
- Rejects authentication in query strings.
- Never logs tokens or evidence payloads.
- Uses read and write timeouts.

### MCP

- Started explicitly with `pi-token-smith mcp`.
- Uses stdio only in v1.
- Writes protocol messages only to stdout.
- Writes diagnostics only to stderr.
- Returns references and metadata before raw content.
- Retrieves exact stored content explicitly through offset and limit pagination.

CLI, HTTP, and MCP must not mask, sanitize, normalize, or summarize stored evidence.

## Failure and recovery semantics

| Situation | Required behavior |
|---|---|
| Daemon unavailable | Pi continues; capture may be lost |
| Socket write fails | Pi continues; record capture loss diagnostics when possible |
| SQLite transaction fails | Roll back the full event ingestion |
| Duplicate event | Return the existing idempotent result |
| Extension disconnects | Preserve committed events; pending exchanges may become incomplete |
| Daemon restarts | Mark abandoned pending exchanges `capture_incomplete` |
| FTS update fails | Roll back event and projections together |
| Integrity check fails | Report failure; never perform destructive automatic repair |

`capture_incomplete` describes incomplete local evidence. It must not be reported as a provider failure.

## Security model

The database intentionally stores complete unencrypted prompts and may contain credentials, personal data, private code, and tool output.

Required controls:

- Local storage only in v1.
- Protected runtime directory and files.
- Same-user Unix socket access.
- Authenticated explicit HTTP.
- No telemetry.
- No evidence in logs.
- No payloads in error messages.
- No database paths supplied by untrusted RPC callers.
- No automatic content disclosure through MCP.
- Explicit raw retrieval with pagination.

Retention is unlimited in v1. Manual project-specific and global deletion commands may be designed later, but no automatic expiration occurs.

## Repository layout

```text
pi-token-smith/
├── cmd/
│   └── pi-token-smith/
│       └── main.go
├── internal/
│   ├── capture/
│   ├── storage/
│   ├── daemon/
│   ├── cli/
│   ├── httpapi/
│   ├── mcp/
│   └── protocol/
├── migrations/
├── extension/
│   ├── index.ts
│   └── protocol.ts
├── docs/
├── go.mod
├── package.json
└── README.md
```

The repository contains both runtimes, but their responsibilities remain separate. The protocol is versioned and released with both components.

## Testing strategy

### Go

Use table-driven tests and `t.TempDir()` for all storage tests.

Required coverage:

- Frame encoding and partial frame decoding.
- Protocol version and operation validation.
- Idempotent event insertion.
- Transaction rollback.
- FTS5 availability, indexing, `MATCH`, and rebuild.
- Hash and payload length integrity.
- Session sequence ordering.
- Incomplete exchange recovery.
- Single-instance daemon locking.
- Stale socket handling.
- RPC request/response correlation.
- HTTP authentication and loopback defaults.
- MCP stdout/stderr separation.
- Raw payload pagination preserving exact bytes.

### TypeScript extension

Required coverage:

- Event-to-envelope mapping.
- Stable correlation hierarchy.
- Monotonic session sequence.
- Daemon health probing and bounded startup attempts.
- No mutation of observed Pi event objects.
- Fail-open behavior for connection and daemon failures.
- No synchronous waiting for persistence acknowledgements.

### Integration

Integration tests should prove:

1. The extension emits a representative captured exchange.
2. The daemon stores the immutable events and projections.
3. Reported usage remains unchanged.
4. FTS5 locates the captured content.
5. CLI, HTTP, and MCP reference the same event.
6. Paginated reads reconstruct the exact stored payload.
7. Daemon failure does not prevent the simulated Pi workflow from completing.

## Acceptance criteria

- [ ] Installing the extension does not modify Pi requests, responses, tools, context, or control flow.
- [ ] The extension starts or reuses exactly one healthy daemon.
- [ ] A real Pi exchange is stored with project, session, turn, and exchange correlation.
- [ ] Stored usage equals the usage reported by Pi.
- [ ] Nested tool, compaction, and branch-summary usage are distinguishable.
- [ ] Immutable payload hashes and lengths verify successfully.
- [ ] FTS5 searches prompts, responses, and tool results.
- [ ] CLI, HTTP, and MCP retrieve the same evidence.
- [ ] Raw pagination reconstructs stored payload bytes exactly.
- [ ] HTTP remains disabled until explicitly started and requires authentication.
- [ ] Daemon or storage failure never prevents Pi from continuing.
- [ ] Abandoned exchanges recover as `capture_incomplete`.
- [ ] `pi-token-smith doctor` diagnoses without modifying evidence.
- [ ] `go test ./...` passes.

## Explicitly deferred

The following work begins only after v1 has collected representative evidence:

- Token optimization reports.
- Deterministic tool recommendations.
- Skill recommendations.
- Secret detection and alerts.
- Token attribution heuristics.
- Savings estimates.
- Encryption at rest.
- Automatic retention or expiration.
- Remote synchronization.
- Graphical interfaces.

## Implementation order

1. Initialize Go and TypeScript project metadata.
2. Define protocol types and framing tests.
3. Implement protected runtime paths and daemon locking.
4. Implement SQLite migrations, immutable events, and FTS5 tests.
5. Implement daemon ingestion and query RPC.
6. Implement the thin Pi extension and fail-open client.
7. Implement CLI diagnostics and evidence queries.
8. Implement authenticated loopback HTTP adapter.
9. Implement MCP stdio adapter.
10. Run end-to-end capture and fidelity verification against Pi.

The v1 milestone ends when evidence collection and retrieval are reliable. Analysis starts only after inspecting the resulting real-world dataset.
