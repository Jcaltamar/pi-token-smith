import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "node:net";
import test from "node:test";

import registerExtension, { CaptureClient, createCaptureState, ensureDaemon, snapshotJSON } from "./index.ts";
import { FrameDecoder, ProtocolVersion, decodeJSON, encodeFrame } from "./protocol.ts";

test("registers passive capture handlers without returning Pi mutations", () => {
  const handlers = new Map();
  registerExtension({ on(name, handler) { handlers.set(name, handler); } });
  const required = ["session_start", "before_agent_start", "turn_start", "before_provider_request", "after_provider_response", "message_end", "tool_call", "tool_result", "turn_end", "session_compact", "session_tree", "agent_end", "session_shutdown", "model_select", "thinking_level_select"];
  assert.deepEqual([...handlers.keys()].sort(), [...required].sort());
  const source = { type: "before_provider_request", payload: { nested: ["unchanged", { usage: 3 }] } };
  const expected = structuredClone(source);
  for (const [name, handler] of handlers) assert.equal(handler(name === "before_provider_request" ? source : { type: name }, fakeContext()), undefined);
  assert.deepEqual(source, expected);
});

test("captures branch summary usage from the registered session_tree handler", () => {
  const handlers = new Map();
  const captures = [];
  registerExtension({ on(name, handler) { handlers.set(name, handler); } }, () => ({
    socketPath: "/unused",
    enqueue(record) { captures.push(record); return true; },
    flush: async () => {},
    close() {},
    health: async () => false,
  }));
  /** @type {import("@earendil-works/pi-coding-agent/dist/core/extensions/types").SessionTreeEvent} */
  const event = {
    type: "session_tree",
    newLeafId: "leaf-new",
    oldLeafId: "leaf-old",
    fromExtension: false,
    summaryEntry: {
      type: "branch_summary",
      id: "summary-1",
      parentId: "leaf-old",
      timestamp: "2026-01-01T00:00:00.000Z",
      fromId: "leaf-old",
      summary: "Branch summary",
      usage: { input: 3, output: 2 },
    },
  };
  assert.equal(handlers.get("session_tree")(event, fakeContext()), undefined);
  assert.equal(captures.length, 1);
  assert.equal(captures[0].event_type, "branch_summary_usage");
  const payload = JSON.parse(new TextDecoder().decode(captures[0].payload));
  assert.equal(payload.evidence_kind, "branch_summary_usage");
  assert.equal(payload.usage_source, "session_tree_summary_entry");
  assert.deepEqual(payload.observed, event);
});

test("snapshots shared references normally, only marks active cycles, and preserves Error fields", () => {
  const shared = { value: 1 };
  const ordinary = { right: shared, left: shared };
  assert.deepEqual(JSON.parse(new TextDecoder().decode(snapshotJSON(ordinary).bytes)), { left: { value: 1 }, right: { value: 1 } });

  const cyclic = { big: 1n, missing: undefined };
  cyclic.self = cyclic;
  const error = new Error("private");
  error.extra = { z: 1 };
  const snapshot = JSON.parse(new TextDecoder().decode(snapshotJSON({ cyclic, error }).bytes));
  assert.equal(snapshot.cyclic.big, "[BigInt:1]");
  assert.equal(snapshot.cyclic.missing, "[Undefined]");
  assert.equal(snapshot.cyclic.self, "[Circular]");
  assert.deepEqual(Object.keys(snapshot.error), ["name", "message", "stack", "extra"]);
  assert.equal(snapshot.error.name, "Error");
  assert.equal(snapshot.error.message, "private");
  assert.match(snapshot.error.stack, /^Error: private/);
  assert.deepEqual(snapshot.error.extra, { z: 1 });
});

test("capture state creates monotonic envelopes with classified evidence and correlation", () => {
  const state = createCaptureState("/workspace", "session-a");
  const first = state.capture("before_provider_request", { payload: { messages: [{ content: "hi" }] } }, fakeContext());
  const assistantUsage = state.capture("message_end", { message: { role: "assistant", usage: { input: 3 } } }, fakeContext());
  const assistant = state.capture("message_end", { message: { role: "assistant", content: "hi" } }, fakeContext());
  const user = state.capture("message_end", { message: { role: "user", content: "hi" } }, fakeContext());
  const toolCall = state.capture("tool_call", { toolCall: { name: "search" } }, fakeContext());
  const toolUsage = state.capture("tool_result", { result: { usage: { output: 2 } } }, fakeContext());
  const toolResult = state.capture("tool_result", { result: { content: "ok" } }, fakeContext());
  const providerMetadata = state.capture("after_provider_response", { responseId: "response-1" }, fakeContext());
  const providerUsage = state.capture("after_provider_response", { usage: { total: 5 } }, fakeContext());
  const compact = state.capture("session_compact", { usage: { total: 6 } }, fakeContext());
  const lifecycle = state.capture("turn_end", { reason: "complete" }, fakeContext());
  assert.equal(first.event_type, "pi_provider_payload_json");
  assert.equal(assistantUsage.event_type, "provider_reported_usage");
  assert.equal(assistant.event_type, "pi_assistant_message_json");
  assert.equal(user.event_type, "pi_user_message_json");
  assert.equal(toolCall.event_type, "pi_tool_call_json");
  assert.equal(toolUsage.event_type, "nested_tool_usage");
  assert.equal(toolResult.event_type, "pi_tool_result_json");
  assert.equal(providerMetadata.event_type, "provider_response_metadata");
  assert.equal(providerUsage.event_type, "provider_reported_usage");
  assert.equal(compact.event_type, "compaction_usage");
  assert.equal(lifecycle.event_type, "lifecycle_json");
  const assistantUsagePayload = JSON.parse(new TextDecoder().decode(assistantUsage.payload));
  assert.equal(assistantUsagePayload.evidence_kind, "provider_reported_usage");
  assert.equal(assistantUsagePayload.usage_source, "pi_assistant_message_json");
  assert.deepEqual(assistantUsagePayload.observed, { message: { role: "assistant", usage: { input: 3 } } });
  assert.equal(first.sequence, 1);
  assert.equal(assistantUsage.sequence, 2);
  assert.equal(first.session_id, "session-a");
  assert.equal(assistantUsage.exchange_id, first.exchange_id);
});

test("CaptureClient sends exact Go-compatible frames serially and reconnects per event", async () => {
  const received = [];
  await withUnixServer(async socketPath => {
    const client = new CaptureClient({ socketPath, progressTimeoutMs: 100 });
    assert.equal(client.enqueue(captureInput("one", "alpha")), true);
    assert.equal(client.enqueue(captureInput("two", "beta")), true);
    await client.flush(500);
  }, (request, evidence) => received.push({ request, evidence }));

  assert.equal(received.length, 2);
  assert.deepEqual(received.map(item => item.request.operation), ["capture.append", "capture.append"]);
  assert.deepEqual(received.map(item => new TextDecoder().decode(item.evidence)), ["alpha", "beta"]);
  assert.deepEqual(received.map(item => item.request.body.sequence), [1, 1]);
});

test("CaptureClient fails open for malformed acknowledgements and accounts in-flight work", async () => {
  await withUnixServer(async socketPath => {
    const client = new CaptureClient({ socketPath, maxPendingEvents: 1, progressTimeoutMs: 100 });
    assert.equal(client.enqueue(captureInput("one", "alpha")), true);
    assert.equal(client.enqueue(captureInput("two", "beta")), false);
    await client.flush(500);
    assert.equal(client.status.pendingEvents, 0);
    assert.equal(client.status.pendingBytes, 0);
    assert.equal(client.status.droppedEvents, 1);
    assert.equal(client.status.transportFailures, 1);
  }, () => "malformed");
});

test("CaptureClient absorbs post-connect errors, drains, and reconnects", async () => {
  let connections = 0;
  const uncaught = [];
  const rejections = [];
  const onUncaught = error => uncaught.push(error);
  const onRejection = reason => rejections.push(reason);
  process.on("uncaughtException", onUncaught);
  process.on("unhandledRejection", onRejection);
  try {
    const client = new CaptureClient({ progressTimeoutMs: 100, connectSocket: () => {
      connections += 1;
      return connections === 1 ? new ErrorBetweenPhasesSocket() : new AcknowledgingSocket();
    } });
    assert.equal(client.enqueue(captureInput("one", "alpha")), true);
    await client.flush(500);
    assert.equal(client.status.pendingEvents, 0);
    assert.equal(client.status.transportFailures, 1);
    assert.equal(client.enqueue(captureInput("two", "beta")), true);
    await client.flush(500);
    assert.equal(client.status.pendingEvents, 0);
    assert.equal(connections, 2);
    await new Promise(resolve => setImmediate(resolve));
    assert.deepEqual(uncaught, []);
    assert.deepEqual(rejections, []);
  } finally {
    process.off("uncaughtException", onUncaught);
    process.off("unhandledRejection", onRejection);
  }
});

test("CaptureClient timeout destroys stalled sockets and bounded flush returns", async () => {
  let closed = false;
  await withUnixServer(async socketPath => {
    const client = new CaptureClient({ socketPath, progressTimeoutMs: 15 });
    assert.equal(client.enqueue(captureInput("one", "alpha")), true);
    await client.flush(100);
    assert.equal(client.status.pendingEvents, 0);
    assert.equal(client.status.transportFailures, 1);
  }, () => false, () => { closed = true; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(closed, true);
});

test("daemon bootstrap avoids spawning when healthy and single-flights unavailable sockets", async () => {
  let spawnCount = 0;
  const healthy = { socketPath: "/healthy", health: async () => true };
  await ensureDaemon(healthy, { spawnDaemon: () => { spawnCount += 1; throw new Error("must not spawn"); } });
  assert.equal(spawnCount, 0);

  let release;
  const waiting = new Promise(resolve => { release = resolve; });
  const unavailable = { socketPath: "/unavailable", health: async () => false };
  const options = {
    spawnDaemon(command, args, spawnOptions) {
      spawnCount += 1;
      assert.equal(command, "/literal/pi-token-smith");
      assert.deepEqual(args, ["daemon"]);
      assert.equal(spawnOptions.shell, false);
      return { unref() {} };
    },
    findBinary: () => "/literal/pi-token-smith",
    sleep: async () => waiting,
  };
  const first = ensureDaemon(unavailable, options);
  const second = ensureDaemon(unavailable, options);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(spawnCount, 1);
  release();
  await Promise.all([first, second]);
});

test("daemon spawn failure fails open and attempts are cleared for retry", async () => {
  const unavailable = { socketPath: "/retry", health: async () => false };
  let attempts = 0;
  const options = {
    findBinary: () => "/literal/pi-token-smith",
    sleep: async () => {},
    spawnDaemon() {
      attempts += 1;
      if (attempts === 1) throw new Error("spawn failed");
      return { unref() {} };
    },
  };
  await ensureDaemon(unavailable, options);
  await ensureDaemon(unavailable, options);
  assert.equal(attempts, 2);
});

async function withUnixServer(run, onRequest, onClose = () => {}) {
  const directory = await mkdtemp(join(tmpdir(), "pi-token-smith-"));
  const socketPath = join(directory, "capture.sock");
  const server = createServer(socket => {
    let buffer = new Uint8Array();
    let request;
    let evidenceLength;
    socket.on("close", onClose);
    socket.on("data", chunk => {
      buffer = concat(buffer, chunk);
      while (true) {
        if (!request) {
          if (buffer.byteLength < 8) return;
          const length = Number(new DataView(buffer.buffer, buffer.byteOffset, 8).getBigUint64(0));
          if (buffer.byteLength < 8 + length) return;
          request = decodeJSON(buffer.slice(8, 8 + length));
          buffer = buffer.slice(8 + length);
          if (request.operation === "system.health") {
            socket.write(encodeFrame({ protocol_version: ProtocolVersion, request_id: request.request_id, status: "ok", body: {} }));
            return;
          }
        }
        if (evidenceLength === undefined) {
          if (buffer.byteLength < 8) return;
          evidenceLength = Number(new DataView(buffer.buffer, buffer.byteOffset, 8).getBigUint64(0));
          buffer = buffer.slice(8);
        }
        if (buffer.byteLength < evidenceLength) return;
        const evidence = buffer.slice(0, evidenceLength);
        buffer = buffer.slice(evidenceLength);
        const response = onRequest(request, evidence);
        if (response === "malformed") socket.write(encodeFrame({ protocol_version: ProtocolVersion, request_id: "wrong-request", status: "ok", body: {} }));
        else if (response !== false) socket.write(encodeFrame({ protocol_version: ProtocolVersion, request_id: request.request_id, status: "ok", body: {} }));
        return;
      }
    });
  });
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(socketPath, resolve); });
  try { await run(socketPath); }
  finally {
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
}

class ErrorBetweenPhasesSocket extends EventEmitter {
  constructor() {
    super();
    queueMicrotask(() => this.emit("connect"));
  }

  write(_bytes, callback) {
    callback();
    queueMicrotask(() => this.emit("error", new Error("between phases")));
    return true;
  }

  destroy() { this.destroyed = true; return this; }
}

class AcknowledgingSocket extends EventEmitter {
  #writes = 0;
  #requestId;

  constructor() {
    super();
    queueMicrotask(() => this.emit("connect"));
  }

  write(bytes, callback) {
    this.#writes += 1;
    if (this.#writes === 1) {
      const length = Number(new DataView(bytes.buffer, bytes.byteOffset, 8).getBigUint64(0));
      this.#requestId = decodeJSON(bytes.slice(8, 8 + length)).request_id;
    }
    callback();
    if (this.#writes === 3) queueMicrotask(() => this.emit("data", encodeFrame({ protocol_version: ProtocolVersion, request_id: this.#requestId, status: "ok", body: {} })));
    return true;
  }
  destroy() { this.destroyed = true; return this; }
}

function captureInput(eventId, payload) {
  return { event_id: eventId, event_type: "lifecycle_json", project_id: "project", session_id: "session", sequence: 1, occurred_at: new Date().toISOString(), payload: new TextEncoder().encode(payload) };
}

function concat(left, right) {
  const result = new Uint8Array(left.byteLength + right.byteLength);
  result.set(left);
  result.set(right, left.byteLength);
  return result;
}

function fakeContext() {
  return { cwd: "/workspace", model: { provider: "test", id: "model" }, thinkingLevel: "medium", sessionManager: { getSessionId: () => "session-a" } };
}
