# Pi Token Smith

Pi Token Smith is a Linux-only, passive Pi extension and local Go daemon that preserves the model-billing evidence Pi can observe. It stores raw evidence in SQLite, indexes it with FTS5, and exposes retrieval through a CLI, an explicit loopback HTTP server, and MCP stdio.

> **v1 focus:** capture and retrieve evidence faithfully. It does not analyze usage or make optimization recommendations.

## Quick start

Requirements: Linux, Go, Node.js, and Pi. This repository has no lockfile; use the checked-in `package.json` with your existing package-manager policy.

```bash
# From this checkout
npm install
mkdir -p "$HOME/.local/bin"
go build -o "$HOME/.local/bin/pi-token-smith" ./cmd/pi-token-smith
export PATH="$HOME/.local/bin:$PATH"

# Load the extension for a Pi session.
pi --extension "$PWD/extension/index.ts"
```

Alternatively, copy `extension/index.ts` to Pi's auto-discovered extension directory:

```bash
cp extension/index.ts "$HOME/.pi/agent/extensions/"
```

The extension looks for `pi-token-smith` on `PATH`, starts a daemon only when its socket is unhealthy, and then continues passively. For a non-standard binary location, set `PI_TOKEN_SMITH_BIN` to its absolute path before starting Pi.

## v1 boundaries

| Included | Deliberately not included |
|---|---|
| Local capture of observable Pi lifecycle, provider, message, and tool events | Model calls, prompt mutation, request blocking, or SQLite access from the extension |
| Immutable SQLite evidence and FTS5 search | Usage reports, optimization recommendations, or savings estimates |
| CLI, authenticated loopback HTTP, and MCP stdio retrieval | Deletion workflows, retention automation, secret detection/alerts, encryption at rest, or remote sync |

See [PLAN.md](PLAN.md) for the complete v1 plan and constraints.

## Architecture and fail-open behavior

```text
Pi events → TypeScript extension → Unix-socket RPC → Go daemon → SQLite + FTS5
                                                     ↘ CLI / HTTP / MCP
```

The TypeScript extension observes events, snapshots JSON-visible values, assigns project/session/correlation metadata, and queues capture envelopes. The Go daemon is the single SQLite owner; it validates RPC, applies migrations, writes immutable events and projections transactionally, and serves retrieval interfaces.

Capture is **fail-open**: unavailable sockets, daemon startup failures, transport failures, and storage failures must not block or alter Pi. Capture can be lost in those cases. The extension has bounded buffering and does not log evidence payloads.

## Runtime and daemon modes

All state is per-user and outside the repository:

```text
~/.pi/agent/pi-token-smith/
├── token-smith.sqlite
├── token-smith.sock
├── token-smith.lock
└── http.token              # created only by `serve`
```

The runtime directory is `0700`; the database, socket, lock, and HTTP token are `0600`.

| Mode | Command | Behavior |
|---|---|---|
| Automatic capture | Start Pi with the extension | The extension probes the socket and boundedly auto-starts `pi-token-smith daemon` when needed. |
| Foreground daemon | `pi-token-smith daemon` | Owns the global database and Unix socket until signalled. |
| Diagnostics | `pi-token-smith status --json` or `pi-token-smith doctor` | Queries health or checks runtime permissions and daemon information. |
| HTTP | `pi-token-smith serve` | Starts an authenticated loopback-only read API; disabled otherwise. |
| MCP | `pi-token-smith mcp` | Serves MCP over stdio; protocol output is stdout and diagnostics are stderr. |

## Retrieval examples

The daemon must be running, normally because the extension started it.

```bash
pi-token-smith status --json
pi-token-smith search --limit 20 'exact phrase'
pi-token-smith inspect EVENT_ID
pi-token-smith inspect --offset 0 --limit 4096 EVENT_ID
pi-token-smith inspect --output ./evidence.json EVENT_ID
```

`search` returns references, not raw content. `inspect` writes the exact stored payload bytes to stdout unless `--output` is specified.

### HTTP

Start the server and retain its printed URL:

```bash
pi-token-smith serve --listen 127.0.0.1:8787
TOKEN=$(cat "$HOME/.pi/agent/pi-token-smith/http.token")
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8787/v1/health
curl -G -H "Authorization: Bearer $TOKEN" \
  --data-urlencode 'q=exact phrase' \
  'http://127.0.0.1:8787/v1/search?limit=20'
curl -H "Authorization: Bearer $TOKEN" \
  'http://127.0.0.1:8787/v1/events/EVENT_ID/payload?offset=0&limit=4096'
```

HTTP accepts only loopback listeners, requires one Bearer token, rejects CORS requests and token query parameters, and exposes `/v1/health`, `/v1/info`, `/v1/search`, and event payload retrieval.

### MCP

Configure your MCP client to launch:

```bash
pi-token-smith mcp
```

Available tools are `token_smith_status`, `token_smith_search`, `token_smith_get_event`, and `token_smith_read_payload`. Search and metadata tools return references first; raw payload reads require an explicit event ID, offset, limit, and `utf8` or lossless `base64` encoding.

## Fidelity and security warnings

- Evidence is a snapshot of what Pi exposes at this extension's position. A provider payload is **not** asserted to be exact HTTP request bytes, and later extensions can mutate it.
- Provider-reported usage is authoritative when Pi exposes it. Nested tool, compaction, and branch-summary usage remain separately classified to avoid accidental double counting.
- Raw evidence is intentionally unmasked and may contain prompts, credentials, personal data, private source code, and tool output. SQLite is not encrypted at rest in v1.
- Protect the runtime directory, database, exported `inspect` files, and `http.token`. Do not send raw payloads to untrusted tools or services.
- Retention is unlimited in v1. There is no implemented deletion command, automatic expiry, secret alert, or report feature.

## Testing

```bash
# TypeScript unit tests
node --test extension/*.test.mjs

# End-to-end: builds a real Go binary, uses a temporary HOME, auto-starts the daemon,
# drives real extension handlers, and verifies SQLite/FTS and CLI search/inspect.
npm run test:e2e

# TypeScript check
tsc --noEmit

# Go validation
go test ./...
go test -race ./...
CGO_ENABLED=0 go test ./...
go vet ./...
```

The E2E harness uses synthetic Pi event payloads only; it makes no model call and removes its temporary binary, daemon runtime, socket, database, and evidence after the test.

## Limitations

This is a v1 evidence collector, not a billing reconciliation system. It only captures information locally observable to the extension, requires Linux, and can lose events under fail-open failures or bounded-buffer pressure. It does not yet offer deletion, reporting, retention controls, encryption, remote synchronization, GUI tooling, or automatic secret alerts.
