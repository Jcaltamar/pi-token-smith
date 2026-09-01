import { spawn, type ChildProcess, type SpawnOptions } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { realpathSync } from "node:fs";
import { homedir } from "node:os";
import { connect, type Socket } from "node:net";
import { delimiter, resolve } from "node:path";

import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";

import { FrameDecoder, ProtocolVersion, decodeJSON, encodeEvidenceHeader, encodeFrame } from "./protocol.ts";

type JSONRecord = Record<string, unknown>;
type CaptureInput = Omit<CaptureRecord, "payload_size" | "encoding"> & { payload: Uint8Array };
type SocketFactory = (path: string) => Socket;
type Sleep = (milliseconds: number) => Promise<void>;
type SpawnDaemon = (command: string, args: readonly string[], options: SpawnOptions) => ChildProcess;

export interface CaptureRecord {
  readonly event_id: string;
  readonly event_type: string;
  readonly project_id: string;
  readonly session_id: string;
  readonly exchange_id?: string;
  readonly sequence: number;
  readonly occurred_at: string;
  readonly encoding: "application/json";
  readonly payload_size: number;
  readonly payload: Uint8Array;
}

export interface CaptureClientOptions {
  readonly socketPath?: string;
  readonly maxPendingEvents?: number;
  readonly maxPendingBytes?: number;
  readonly dialTimeoutMs?: number;
  readonly progressTimeoutMs?: number;
  /** Injectable transport and clock seams for deterministic embedding tests. */
  readonly connectSocket?: SocketFactory;
  readonly now?: () => number;
  readonly sleep?: Sleep;
}

export interface CaptureStatus {
  readonly pendingEvents: number;
  readonly pendingBytes: number;
  readonly droppedEvents: number;
  readonly droppedBytes: number;
  readonly transportFailures: number;
}

export interface DaemonBootstrapOptions {
  readonly socketPath?: string;
  readonly findBinary?: (binary: string) => string | undefined;
  readonly spawnDaemon?: SpawnDaemon;
  readonly sleep?: Sleep;
}

const runtimeSocket = resolve(homedir(), ".pi/agent/pi-token-smith/token-smith.sock");
const daemonAttempts = new Map<string, Promise<void>>();
const socketFaults = new WeakMap<Socket, Error>();

/** A bounded, fail-open client. It deliberately has no logging surface. */
export class CaptureClient {
  #queue: CaptureRecord[] = [];
  #pendingEvents = 0;
  #pendingBytes = 0;
  #droppedEvents = 0;
  #droppedBytes = 0;
  #transportFailures = 0;
  #draining = false;
  #closed = false;
  readonly #options: Required<Omit<CaptureClientOptions, "connectSocket" | "now" | "sleep">> & Pick<CaptureClientOptions, "connectSocket" | "now" | "sleep">;

  constructor(options: CaptureClientOptions = {}) {
    this.#options = {
      socketPath: options.socketPath ?? runtimeSocket,
      maxPendingEvents: options.maxPendingEvents ?? 128,
      maxPendingBytes: options.maxPendingBytes ?? 4 * 1024 * 1024,
      dialTimeoutMs: options.dialTimeoutMs ?? 150,
      progressTimeoutMs: options.progressTimeoutMs ?? 500,
      connectSocket: options.connectSocket,
      now: options.now,
      sleep: options.sleep,
    };
  }

  get socketPath(): string { return this.#options.socketPath; }

  get status(): CaptureStatus {
    return { pendingEvents: this.#pendingEvents, pendingBytes: this.#pendingBytes, droppedEvents: this.#droppedEvents, droppedBytes: this.#droppedBytes, transportFailures: this.#transportFailures };
  }

  enqueue(input: CaptureInput): boolean {
    const event = { ...input, encoding: "application/json" as const, payload_size: input.payload.byteLength };
    if (this.#closed || event.payload_size > this.#options.maxPendingBytes || this.#pendingEvents >= this.#options.maxPendingEvents || this.#pendingBytes + event.payload_size > this.#options.maxPendingBytes) {
      this.#droppedEvents += 1;
      this.#droppedBytes += event.payload_size;
      return false;
    }
    this.#queue.push(event);
    this.#pendingEvents += 1;
    this.#pendingBytes += event.payload_size;
    void this.#drain();
    return true;
  }

  async flush(timeoutMs = this.#options.progressTimeoutMs): Promise<void> {
    const deadline = this.#now() + timeoutMs;
    while ((this.#draining || this.#queue.length > 0) && this.#now() < deadline) await this.#sleep(5);
  }

  close(): void { this.#closed = true; }

  async health(): Promise<boolean> {
    try {
      const response = await this.#request({ protocol_version: ProtocolVersion, request_id: randomUUID(), operation: "system.health", sent_at: new Date().toISOString(), body: {} }, undefined);
      return response.status === "ok";
    } catch { this.#transportFailures += 1; return false; }
  }

  async #drain(): Promise<void> {
    if (this.#draining) return;
    this.#draining = true;
    try {
      while (this.#queue.length > 0) {
        const event = this.#queue.shift()!;
        try {
          await this.#request({ protocol_version: ProtocolVersion, request_id: randomUUID(), operation: "capture.append", sent_at: new Date().toISOString(), body: captureBody(event) }, event.payload);
        } catch { this.#transportFailures += 1; }
        finally {
          this.#pendingEvents -= 1;
          this.#pendingBytes -= event.payload_size;
        }
      }
    } finally { this.#draining = false; }
  }

  async #request(request: JSONRecord, evidence: Uint8Array | undefined): Promise<{ status: string }> {
    const socket = await this.#dial();
    try {
      await writeAll(socket, encodeFrame(request as never), this.#options.progressTimeoutMs);
      if (evidence) {
        await writeAll(socket, encodeEvidenceHeader(BigInt(evidence.byteLength)), this.#options.progressTimeoutMs);
        await writeAll(socket, evidence, this.#options.progressTimeoutMs);
      }
      const response = await readResponse(socket, this.#options.progressTimeoutMs);
      if (response.protocol_version !== ProtocolVersion || response.request_id !== request.request_id || response.status !== "ok") throw new Error("invalid capture acknowledgement");
      return response as { status: string };
    } finally { socket.destroy(); }
  }

  #dial(): Promise<Socket> {
    return new Promise((resolveDial, rejectDial) => {
      const socket = (this.#options.connectSocket ?? connect)(this.#options.socketPath);
      retainSocketErrorListener(socket);
      let settled = false;
      const cleanup = () => { clearTimeout(timer); socket.off("connect", onConnect); socket.off("error", onError); };
      const finish = (result: () => void) => { if (!settled) { settled = true; cleanup(); result(); } };
      const onConnect = () => finish(() => resolveDial(socket));
      const onError = (error: Error) => finish(() => rejectDial(error));
      const timer = setTimeout(() => finish(() => { poisonSocket(socket, new Error("capture dial timeout")); rejectDial(new Error("capture dial timeout")); }), this.#options.dialTimeoutMs);
      socket.once("connect", onConnect);
      socket.once("error", onError);
    });
  }

  #now(): number { return (this.#options.now ?? Date.now)(); }
  #sleep(milliseconds: number): Promise<void> { return (this.#options.sleep ?? delay)(milliseconds); }
}

export function snapshotJSON(value: unknown): { readonly bytes: Uint8Array; readonly serialization_failure?: string } {
  try { return { bytes: new TextEncoder().encode(JSON.stringify(snapshotValue(value, new WeakSet()))) }; }
  catch { return { bytes: new TextEncoder().encode('{"serialization_failure":"unsupported_value"}'), serialization_failure: "unsupported_value" }; }
}

function snapshotValue(value: unknown, stack: WeakSet<object>): unknown {
  if (value === undefined) return "[Undefined]";
  if (typeof value === "bigint") return `[BigInt:${value}]`;
  if (typeof value === "function") return "[Unsupported:function]";
  if (typeof value === "symbol") return "[Unsupported:symbol]";
  if (value === null || typeof value !== "object") return value;
  if (stack.has(value)) return "[Circular]";
  stack.add(value);
  try {
    if (value instanceof Error) {
      const result: Record<string, unknown> = { name: value.name, message: value.message, stack: value.stack };
      for (const key of Object.keys(value).sort()) if (!(key in result)) result[key] = snapshotValue((value as unknown as Record<string, unknown>)[key], stack);
      return result;
    }
    if (Array.isArray(value)) return value.map(item => snapshotValue(item, stack));
    const result: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      try { result[key] = snapshotValue((value as Record<string, unknown>)[key], stack); }
      catch { result[key] = "[Unsupported:property]"; }
    }
    return result;
  } finally { stack.delete(value); }
}

export function createCaptureState(cwd: string, sessionId: string) {
  const canonicalPath = canonicalProjectPath(cwd);
  const projectId = createHash("sha256").update(canonicalPath).digest("hex");
  let sequence = 0;
  let agentRunId: string | undefined;
  let turnId: string | undefined;
  let exchangeId: string | undefined;
  return {
    capture(eventName: string, event: unknown, ctx: unknown): CaptureRecord {
      if (eventName === "before_agent_start") agentRunId = randomUUID();
      if (eventName === "turn_start") turnId = randomUUID();
      if (eventName === "before_provider_request") exchangeId = randomUUID();
      const correlation: JSONRecord = {};
      if (agentRunId) correlation.agent_run_id = agentRunId;
      if (turnId) correlation.turn_id = turnId;
      if (exchangeId) correlation.exchange_id = exchangeId;
      const evidence = classifyEvidence(eventName, event);
      const snapshot = snapshotJSON({ event_type: eventName, evidence_kind: evidence.event_type, ...(evidence.usage_source ? { usage_source: evidence.usage_source } : {}), observed: event, context: contextMetadata(ctx, sessionId), correlation });
      return { event_id: randomUUID(), event_type: evidence.event_type, project_id: projectId, session_id: sessionId, exchange_id: exchangeId, sequence: ++sequence, occurred_at: new Date().toISOString(), encoding: "application/json", payload_size: snapshot.bytes.byteLength, payload: snapshot.bytes };
    },
  };
}

type CaptureClientFactory = () => Pick<CaptureClient, "enqueue" | "flush" | "close" | "health" | "socketPath">;

export default function registerExtension(pi: ExtensionAPI, createClient: CaptureClientFactory = () => new CaptureClient()): void {
  let client = createClient();
  let state = createCaptureState(process.cwd(), "unknown");
  const observe = (eventName: string) => (event: unknown, ctx: ExtensionContext): undefined => {
    if (eventName === "session_start") {
      client = createClient();
      const sessionId = ctx.sessionManager.getSessionId();
      state = createCaptureState(ctx.cwd, sessionId);
      void ensureDaemon(client);
    }
    client.enqueue(state.capture(eventName, event, ctx));
    if (eventName === "session_shutdown") void client.flush(75).finally(() => client.close());
    return undefined;
  };
  pi.on("session_start", observe("session_start"));
  pi.on("before_agent_start", observe("before_agent_start"));
  pi.on("turn_start", observe("turn_start"));
  pi.on("before_provider_request", observe("before_provider_request"));
  pi.on("after_provider_response", observe("after_provider_response"));
  pi.on("message_end", observe("message_end"));
  pi.on("tool_call", observe("tool_call"));
  pi.on("tool_result", observe("tool_result"));
  pi.on("turn_end", observe("turn_end"));
  pi.on("session_compact", observe("session_compact"));
  pi.on("session_tree", observe("session_tree"));
  pi.on("agent_end", observe("agent_end"));
  pi.on("session_shutdown", observe("session_shutdown"));
  pi.on("model_select", observe("model_select"));
  pi.on("thinking_level_select", observe("thinking_level_select"));
}

function classifyEvidence(name: string, event: unknown): { event_type: string; usage_source?: string } {
  const messageRole = (event as { message?: { role?: unknown } })?.message?.role;
  const usage = findUsage(event);
  if (name === "before_provider_request") return { event_type: "pi_provider_payload_json" };
  if (name === "after_provider_response") return usage ? { event_type: "provider_reported_usage", usage_source: "provider_response_metadata" } : { event_type: "provider_response_metadata" };
  if (name === "message_end" && messageRole === "assistant") return usage ? { event_type: "provider_reported_usage", usage_source: "pi_assistant_message_json" } : { event_type: "pi_assistant_message_json" };
  if (name === "message_end" && messageRole === "user") return { event_type: "pi_user_message_json" };
  if (name === "message_end" && messageRole === "branchSummary") return { event_type: "branch_summary_usage", usage_source: "pi_branch_summary_message_json" };
  if (name === "tool_call") return { event_type: "pi_tool_call_json" };
  if (name === "tool_result") return usage ? { event_type: "nested_tool_usage", usage_source: "pi_tool_result_json" } : { event_type: "pi_tool_result_json" };
  if (name === "session_compact" && usage) return { event_type: "compaction_usage", usage_source: "session_compact" };
  if (name === "session_tree" && hasBranchSummaryUsage(event)) return { event_type: "branch_summary_usage", usage_source: "session_tree_summary_entry" };
  return { event_type: "lifecycle_json" };
}

function hasBranchSummaryUsage(event: unknown): boolean {
  const summaryEntry = (event as { summaryEntry?: { type?: unknown; usage?: unknown } })?.summaryEntry;
  return summaryEntry?.type === "branch_summary" && summaryEntry.usage !== undefined;
}

function findUsage(value: unknown, seen = new WeakSet<object>()): boolean {
  if (!value || typeof value !== "object") return false;
  if (seen.has(value)) return false;
  seen.add(value);
  const record = value as JSONRecord;
  for (const key of Object.keys(record)) {
    try {
      const item = record[key];
      if (key === "usage" && item !== undefined) return true;
      if (findUsage(item, seen)) return true;
    } catch { /* ignore inaccessible payload fields */ }
  }
  return false;
}

function contextMetadata(ctx: unknown, sessionId: string): JSONRecord {
  const value = ctx as { cwd?: unknown; model?: unknown; thinkingLevel?: unknown };
  return { cwd: value?.cwd, model: value?.model, thinking_level: value?.thinkingLevel, session_id: sessionId };
}

function canonicalProjectPath(cwd: string): string { try { return realpathSync(cwd); } catch { return resolve(cwd); } }
function captureBody(event: CaptureRecord): JSONRecord { const { payload, ...body } = event; return body; }
function delay(milliseconds: number): Promise<void> { return new Promise(resolveDelay => setTimeout(resolveDelay, milliseconds)); }

function retainSocketErrorListener(socket: Socket): void {
  socket.on("error", error => poisonSocket(socket, error));
}

function poisonSocket(socket: Socket, error: Error): void {
  if (!socketFaults.has(socket)) socketFaults.set(socket, error);
  socket.destroy();
}

function socketFault(socket: Socket): Error | undefined { return socketFaults.get(socket); }

function writeAll(socket: Socket, bytes: Uint8Array, timeoutMs: number): Promise<void> {
  return new Promise((resolveWrite, rejectWrite) => {
    const fault = socketFault(socket);
    if (fault) { rejectWrite(fault); return; }
    let settled = false;
    const cleanup = () => { clearTimeout(timer); socket.off("error", onError); };
    const finish = (result: () => void) => { if (!settled) { settled = true; cleanup(); result(); } };
    const onError = (error: Error) => finish(() => { poisonSocket(socket, error); rejectWrite(error); });
    const timer = setTimeout(() => finish(() => { const error = new Error("capture progress timeout"); poisonSocket(socket, error); rejectWrite(error); }), timeoutMs);
    socket.once("error", onError);
    socket.write(bytes, error => finish(() => {
      if (error) { poisonSocket(socket, error); rejectWrite(error); }
      else if (socketFault(socket)) rejectWrite(socketFault(socket)!);
      else resolveWrite();
    }));
  });
}

function readResponse(socket: Socket, timeoutMs: number): Promise<JSONRecord> {
  return new Promise((resolveRead, rejectRead) => {
    const fault = socketFault(socket);
    if (fault) { rejectRead(fault); return; }
    const decoder = new FrameDecoder();
    let settled = false;
    const cleanup = () => { clearTimeout(timer); socket.off("data", onData); socket.off("error", onError); socket.off("end", onEnd); socket.off("close", onClose); };
    const finish = (result: () => void) => { if (!settled) { settled = true; cleanup(); result(); } };
    const onData = (chunk: Uint8Array) => { try { const frames = decoder.push(chunk); if (frames.length) finish(() => resolveRead(decodeJSON(frames[0]) as JSONRecord)); } catch (error) { finish(() => rejectRead(error)); } };
    const onError = (error: Error) => finish(() => { poisonSocket(socket, error); rejectRead(error); });
    const onEnd = () => finish(() => rejectRead(new Error("capture socket closed before acknowledgement")));
    const onClose = () => finish(() => rejectRead(new Error("capture socket closed before acknowledgement")));
    const timer = setTimeout(() => finish(() => { const error = new Error("capture progress timeout"); poisonSocket(socket, error); rejectRead(error); }), timeoutMs);
    socket.on("data", onData);
    socket.once("error", onError);
    socket.once("end", onEnd);
    socket.once("close", onClose);
  });
}

export async function ensureDaemon(client: Pick<CaptureClient, "health" | "socketPath">, options: DaemonBootstrapOptions = {}): Promise<void> {
  if (await client.health()) return;
  const socketPath = options.socketPath ?? client.socketPath;
  const existing = daemonAttempts.get(socketPath);
  if (existing) return existing;
  const attempt = (async () => {
    try {
      const command = process.env.PI_TOKEN_SMITH_BIN || (options.findBinary ?? findPathBinary)("pi-token-smith");
      if (!command) return;
      const child = (options.spawnDaemon ?? spawn)(command, ["daemon"], { detached: true, stdio: "ignore", shell: false });
      child.unref();
      for (let retry = 0; retry < 3; retry += 1) { await (options.sleep ?? delay)(50); if (await client.health()) return; }
    } catch { /* fail open */ }
  })();
  daemonAttempts.set(socketPath, attempt);
  try { await attempt; }
  finally { if (daemonAttempts.get(socketPath) === attempt) daemonAttempts.delete(socketPath); }
}

function findPathBinary(binary: string): string | undefined {
  for (const directory of (process.env.PATH ?? "").split(delimiter)) {
    const candidate = resolve(directory || ".", binary);
    try { realpathSync(candidate); return candidate; } catch { /* continue */ }
  }
  return undefined;
}
