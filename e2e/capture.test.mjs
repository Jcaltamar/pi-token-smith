import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, realpath, rm, stat } from "node:fs/promises";
import { request } from "node:http";
import { tmpdir } from "node:os";
import { PassThrough } from "node:stream";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const repository = resolve(dirname(new URL(import.meta.url).pathname), "..");
const marker = "pi-token-smith-e2e-marker";
const sessionID = "e2e-session-001";
const projectDirectory = join(repository, "extension");

async function runCLI(binary, home, args) {
  const { stdout } = await execFileAsync(binary, args, {
    cwd: repository,
    env: { ...process.env, HOME: home },
    encoding: "utf8",
  });
  return stdout;
}

async function eventually(action, description, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    try { return await action(); }
    catch (error) { lastError = error; await new Promise(resolveDelay => setTimeout(resolveDelay, 25)); }
  }
  throw new Error(`${description}: ${lastError?.message ?? "timed out"}`);
}

async function waitForSearch(binary, home, query) {
  return eventually(async () => {
    const references = JSON.parse(await runCLI(binary, home, ["search", "--limit", "100", query]));
    assert.ok(references.length > 0, `no search result for ${query}`);
    return references;
  }, `wait for ${query}`);
}

function spawnChild(binary, home, args) {
  const child = spawn(binary, args, {
    cwd: repository,
    env: { ...process.env, HOME: home },
    shell: false,
    stdio: ["pipe", "pipe", "pipe"],
  });
  child.exitResult = new Promise(resolveExit => child.once("close", (code, signal) => resolveExit({ code, signal })));
  return child;
}

async function waitForExit(child, description, timeoutMs = 5_000) {
  return Promise.race([
    child.exitResult,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`${description}: timed out`)), timeoutMs)),
  ]);
}

async function stopChild(child, description, closeInput = false) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  if (closeInput) child.stdin.end();
  else child.kill("SIGTERM");
  await waitForExit(child, description);
}

function lineReader(stream, description) {
  let pending = "";
  const lines = [];
  const waiters = [];
  let ended = false;
  const flush = () => {
    while (lines.length && waiters.length) waiters.shift().resolve(lines.shift());
  };
  stream.setEncoding("utf8");
  stream.on("data", chunk => {
    pending += chunk;
    let newline;
    while ((newline = pending.indexOf("\n")) >= 0) {
      lines.push(pending.slice(0, newline));
      pending = pending.slice(newline + 1);
    }
    flush();
  });
  stream.once("end", () => {
    ended = true;
    if (pending) lines.push(pending);
    flush();
    while (waiters.length) waiters.shift().reject(new Error(`${description}: stdout ended`));
  });
  return {
    async next(timeoutMs = 5_000) {
      if (lines.length) return lines.shift();
      if (ended) throw new Error(`${description}: stdout ended`);
      return new Promise((resolveLine, reject) => {
        const waiter = { resolve: resolveLine, reject };
        waiters.push(waiter);
        setTimeout(() => {
          const index = waiters.indexOf(waiter);
          if (index >= 0) {
            waiters.splice(index, 1);
            reject(new Error(`${description}: timed out waiting for stdout`));
          }
        }, timeoutMs);
      });
    },
  };
}

function captureChildOutput(child) {
  const output = { stdout: "", stderr: "" };
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", chunk => { output.stdout += chunk; });
  child.stderr.on("data", chunk => { output.stderr += chunk; });
  return output;
}

function requestHTTP(url, headers = {}) {
  return new Promise((resolveResponse, reject) => {
    const req = request(url, { headers }, response => {
      const chunks = [];
      response.on("data", chunk => chunks.push(chunk));
      response.once("end", () => resolveResponse({ status: response.statusCode, headers: response.headers, trailers: response.trailers, body: Buffer.concat(chunks) }));
    });
    req.once("error", reject);
    req.end();
  });
}

async function readBoundURL(child) {
  const output = lineReader(child.stdout, "HTTP server startup");
  const url = await output.next();
  assert.match(url, /^http:\/\/127\.0\.0\.1:\d+$/, "serve stdout must contain only its bound loopback URL");
  return url;
}

function mcpResponseReader(output) {
  const pendingResponses = new Map();
  return {
    async receive(id) {
      if (pendingResponses.has(id)) {
        const response = pendingResponses.get(id);
        pendingResponses.delete(id);
        return response;
      }
      for (;;) {
        const line = await output.next();
        let message;
        try { message = JSON.parse(line); }
        catch { throw new Error("MCP stdout contained non-JSON-RPC output"); }
        if (!message || typeof message !== "object" || Array.isArray(message)) throw new Error("MCP stdout contained non-JSON-RPC output");
        assert.equal(message.jsonrpc, "2.0", "MCP stdout must contain only JSON-RPC messages");
        if (!Object.hasOwn(message, "id")) {
          assert.equal(typeof message.method, "string", "MCP JSON-RPC notifications must include a method");
          continue;
        }
        if (message.id === id) return message;
        pendingResponses.set(message.id, message);
      }
    },
  };
}

function mcpClient(child) {
  const responses = mcpResponseReader(lineReader(child.stdout, "MCP server"));
  let nextID = 1;
  return {
    async request(method, params) {
      const id = nextID++;
      child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
      const response = await responses.receive(id);
      assert.equal(response.error, undefined, `MCP ${method} failed: ${JSON.stringify(response.error)}`);
      return response.result;
    },
    notify(method, params) {
      child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", method, params })}\n`);
    },
  };
}

function mcpToolJSON(result) {
  assert.notEqual(result.isError, true, `MCP tool error: ${JSON.stringify(result)}`);
  assert.equal(result.content?.length, 1);
  assert.equal(result.content[0].type, "text");
  return JSON.parse(result.content[0].text);
}

function parseLinuxProcessStartTime(contents) {
  const closingParenthesis = contents.lastIndexOf(")");
  const fields = contents.slice(closingParenthesis + 2).trim().split(/\s+/);
  const startTime = fields[19]; // Field 22; fields begin at process state (field 3).
  if (closingParenthesis < 0 || !/^\d+$/.test(startTime)) {
    throw new Error("could not parse Linux process start time");
  }
  return startTime;
}

async function readLockPID(runtime) {
  try {
    const lock = JSON.parse(await readFile(join(runtime, "token-smith.lock"), "utf8"));
    if (!Number.isSafeInteger(lock.pid) || lock.pid <= 0) {
      throw new Error("lock file does not contain a positive integer PID");
    }
    return lock.pid;
  } catch (error) {
    if (error?.code === "ENOENT") return undefined;
    throw error;
  }
}

async function readDaemonIdentity(binary, pid) {
  const [expectedExecutable, executable, processStat] = await Promise.all([
    realpath(binary),
    realpath(`/proc/${pid}/exe`),
    readFile(`/proc/${pid}/stat`, "utf8"),
  ]);
  assert.equal(executable, expectedExecutable, `lock PID ${pid} is not the temporary pi-token-smith binary`);
  return { pid, executable: expectedExecutable, startTime: parseLinuxProcessStartTime(processStat) };
}

function processIsAbsent(pid) {
  try {
    process.kill(pid, 0);
    return false;
  } catch (error) {
    if (error?.code === "ESRCH") return true;
    throw error;
  }
}

async function waitForDaemonShutdown(runtime, pid) {
  await eventually(async () => {
    if (pid !== undefined) assert.ok(processIsAbsent(pid), `daemon PID ${pid} is still running`);
    await assert.rejects(stat(join(runtime, "token-smith.sock")));
  }, "wait for confirmed daemon exit and socket cleanup");
}

async function stopDaemonSafely(runtime, binary, capturedIdentity) {
  const lockPID = await readLockPID(runtime);
  if (lockPID === undefined) {
    await waitForDaemonShutdown(runtime);
    return;
  }

  let liveIdentity;
  try {
    liveIdentity = await readDaemonIdentity(binary, lockPID);
  } catch (error) {
    if (processIsAbsent(lockPID)) {
      await waitForDaemonShutdown(runtime, lockPID);
      return;
    }
    throw new Error(`refusing to signal lock PID ${lockPID}: daemon identity could not be proven (${error.message})`);
  }
  if (capturedIdentity && (liveIdentity.pid !== capturedIdentity.pid || liveIdentity.executable !== capturedIdentity.executable || liveIdentity.startTime !== capturedIdentity.startTime)) {
    throw new Error(`refusing to signal lock PID ${lockPID}: daemon identity changed since bootstrap`);
  }

  try {
    process.kill(lockPID, "SIGTERM");
  } catch (error) {
    if (error?.code !== "ESRCH") throw error;
  }
  await waitForDaemonShutdown(runtime, lockPID);
}

test("parses Linux process start time without being confused by parentheses in the command", () => {
  assert.equal(parseLinuxProcessStartTime("123 (pi-token)smith) S 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 4242"), "4242");
});

test("matches MCP responses by JSON-RPC id around notifications and rejects malformed stdout", async () => {
  const stream = new PassThrough();
  const responses = mcpResponseReader(lineReader(stream, "synthetic MCP server"));
  stream.end([
    JSON.stringify({ jsonrpc: "2.0", method: "notifications/progress", params: { progress: 1 } }),
    JSON.stringify({ jsonrpc: "2.0", id: 2, result: { value: "second" } }),
    JSON.stringify({ jsonrpc: "2.0", method: "notifications/progress", params: { progress: 2 } }),
    JSON.stringify({ jsonrpc: "2.0", id: 1, result: { value: "first" } }),
  ].join("\n") + "\n");
  assert.deepEqual(await responses.receive(1), { jsonrpc: "2.0", id: 1, result: { value: "first" } });
  assert.deepEqual(await responses.receive(2), { jsonrpc: "2.0", id: 2, result: { value: "second" } });

  const malformed = new PassThrough();
  const malformedResponses = mcpResponseReader(lineReader(malformed, "malformed MCP server"));
  malformed.end("not JSON-RPC\n");
  await assert.rejects(malformedResponses.receive(1), /MCP stdout contained non-JSON-RPC output/);
});

test("captures a synthetic Pi session across the extension, daemon, SQLite, FTS5, and CLI", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-token-smith-e2e-"));
  const home = join(temp, "home");
  const binary = join(temp, "pi-token-smith");
  const runtime = join(home, ".pi", "agent", "pi-token-smith");
  const originalHome = process.env.HOME;
  let daemonIdentity;
  let httpChild;
  let mcpChild;
  let httpURL;
  let cleanupConfirmed = false;

  try {
    await mkdir(home, { recursive: true });
    await execFileAsync("go", ["build", "-o", binary, "./cmd/pi-token-smith"], { cwd: repository, encoding: "utf8" });
    process.env.HOME = home;
    process.env.PI_TOKEN_SMITH_BIN = binary;
    assert.equal(process.env.HOME, home, "the harness must use its temporary HOME");

    const { default: registerExtension } = await import("../extension/index.ts");
    const handlers = new Map();
    registerExtension({ on(name, handler) { handlers.set(name, handler); } });
    const context = {
      cwd: projectDirectory,
      model: { provider: "e2e", id: "synthetic-model" },
      thinkingLevel: "high",
      sessionManager: { getSessionId: () => sessionID },
    };
    const emit = (name, payload) => {
      assert.equal(handlers.get(name)(payload, context), undefined, `${name} must remain passive`);
    };

    emit("session_start", { type: "session_start", marker: `${marker}-session-start` });
    await eventually(async () => {
      const health = JSON.parse(await runCLI(binary, home, ["status", "--json"]));
      assert.equal(health.status, "healthy");
      return health;
    }, "wait for bootstrapped daemon health");
    const lockPID = await readLockPID(runtime);
    assert.notEqual(lockPID, undefined, "bootstrap must create a daemon lock");
    daemonIdentity = await readDaemonIdentity(binary, lockPID);

    emit("before_agent_start", { prompt: `${marker}-agent` });
    emit("turn_start", { turn: 1 });
    emit("before_provider_request", { marker: `${marker}-provider`, messages: [{ role: "user", content: "synthetic prompt" }] });
    emit("message_end", { message: { role: "assistant", content: `${marker}-assistant`, usage: { input: 11, output: 7 } } });
    emit("tool_result", { result: { marker: `${marker}-tool`, usage: { input: 3, output: 2 } } });
    emit("session_shutdown", { type: "session_shutdown", marker: `${marker}-shutdown` });

    const providerReferences = await waitForSearch(binary, home, `"${marker}-provider"`);
    const assistantReferences = await waitForSearch(binary, home, `"${marker}-assistant"`);
    const toolReferences = await waitForSearch(binary, home, `"${marker}-tool"`);
    await waitForSearch(binary, home, `"${marker}-shutdown"`);

    const provider = providerReferences[0];
    const assistant = assistantReferences[0];
    const tool = toolReferences[0];
    const providerPayload = JSON.parse(await runCLI(binary, home, ["inspect", provider.id]));
    const assistantPayload = JSON.parse(await runCLI(binary, home, ["inspect", assistant.id]));
    const toolPayload = JSON.parse(await runCLI(binary, home, ["inspect", tool.id]));
    const expectedProjectID = createHash("sha256").update(projectDirectory).digest("hex");

    for (const reference of [provider, assistant, tool]) {
      assert.equal(reference.project_id, expectedProjectID);
      assert.equal(reference.session_id, sessionID);
    }
    assert.equal(providerPayload.evidence_kind, "pi_provider_payload_json");
    assert.equal(providerPayload.observed.marker, `${marker}-provider`);
    assert.equal(assistantPayload.evidence_kind, "provider_reported_usage");
    assert.equal(assistantPayload.usage_source, "pi_assistant_message_json");
    assert.equal(assistantPayload.observed.message.content, `${marker}-assistant`);
    assert.deepEqual(assistantPayload.observed.message.usage, { input: 11, output: 7 });
    assert.equal(toolPayload.evidence_kind, "nested_tool_usage");
    assert.equal(toolPayload.usage_source, "pi_tool_result_json");
    assert.equal(toolPayload.observed.result.marker, `${marker}-tool`);
    assert.deepEqual(toolPayload.observed.result.usage, { input: 3, output: 2 });
    assert.equal(provider.exchange_id, assistant.exchange_id);
    assert.equal(assistant.exchange_id, tool.exchange_id);
    assert.ok(provider.exchange_id);
    assert.ok(provider.sequence < assistant.sequence && assistant.sequence < tool.sequence);

    const cliPayloadBytes = Buffer.from(await runCLI(binary, home, ["inspect", provider.id]));
    const cliPayloadHash = createHash("sha256").update(cliPayloadBytes).digest("hex");
    const cliReference = provider;
    const eventReferenceFields = ["id", "project_id", "session_id", "exchange_id", "sequence"];
    const assertSameReference = reference => {
      for (const field of eventReferenceFields) assert.equal(reference[field], cliReference[field], `EventReference.${field} must match the CLI search result`);
    };

    httpChild = spawnChild(binary, home, ["serve", "--listen", "127.0.0.1:0"]);
    const httpOutput = captureChildOutput(httpChild);
    httpURL = await readBoundURL(httpChild);
    const httpToken = (await readFile(join(runtime, "http.token"), "utf8")).trim();
    const lockOwnerToken = JSON.parse(await readFile(join(runtime, "token-smith.lock"), "utf8")).owner_token;
    assert.equal(typeof lockOwnerToken, "string");
    const authorization = { Authorization: `Bearer ${httpToken}` };
    const httpSearch = await requestHTTP(`${httpURL}/v1/search?q=${encodeURIComponent(`\"${marker}-provider\"`)}&limit=100`, authorization);
    assert.equal(httpSearch.status, 200);
    const httpReference = JSON.parse(httpSearch.body).events.find(reference => reference.id === cliReference.id);
    assert.ok(httpReference, "HTTP search did not return the CLI event");
    assertSameReference(httpReference);
    const httpPayload = await requestHTTP(`${httpURL}/v1/events/${encodeURIComponent(provider.id)}/payload?offset=0&limit=${cliPayloadBytes.length}`, authorization);
    assert.equal(httpPayload.status, 200);
    assert.deepEqual(httpPayload.body, cliPayloadBytes);
    assert.equal(httpPayload.trailers["x-payload-size"], String(cliPayloadBytes.length));
    assert.equal(httpPayload.trailers["x-payload-sha256"], cliPayloadHash);
    assert.equal(httpPayload.trailers["x-payload-offset"], "0");
    assert.equal(httpPayload.trailers["x-payload-bytes"], String(cliPayloadBytes.length));
    const unauthorized = await requestHTTP(`${httpURL}/v1/search?q=${encodeURIComponent(marker)}`);
    assert.equal(unauthorized.status, 401);
    assert.equal(unauthorized.headers["access-control-allow-origin"], undefined);

    mcpChild = spawnChild(binary, home, ["mcp"]);
    const mcpOutput = captureChildOutput(mcpChild);
    const mcp = mcpClient(mcpChild);
    const initialized = await mcp.request("initialize", { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "e2e", version: "1" } });
    assert.equal(initialized.protocolVersion, "2024-11-05");
    mcp.notify("notifications/initialized", {});
    const toolsList = await mcp.request("tools/list", {});
    const toolNames = toolsList.tools.map(tool => tool.name);
    for (const name of ["token_smith_search", "token_smith_get_event", "token_smith_read_payload"]) assert.ok(toolNames.includes(name));
    const mcpSearch = mcpToolJSON(await mcp.request("tools/call", { name: "token_smith_search", arguments: { query: `\"${marker}-provider\"`, limit: 100 } }));
    const mcpReference = mcpSearch.events.find(reference => reference.id === cliReference.id);
    assert.ok(mcpReference, "MCP search did not return the CLI event");
    assertSameReference(mcpReference);
    const mcpEvent = mcpToolJSON(await mcp.request("tools/call", { name: "token_smith_get_event", arguments: { event_id: provider.id } }));
    assert.equal(mcpEvent.event_id, provider.id);
    assert.equal(mcpEvent.total_size, cliPayloadBytes.length);
    assert.equal(mcpEvent.sha256, cliPayloadHash);
    const mcpPayload = mcpToolJSON(await mcp.request("tools/call", { name: "token_smith_read_payload", arguments: { event_id: provider.id, offset: 0, limit: cliPayloadBytes.length, encoding: "utf8" } }));
    assert.equal(mcpPayload.event_id, provider.id);
    assert.equal(mcpPayload.content, cliPayloadBytes.toString("utf8"));
    assert.equal(mcpPayload.total_size, cliPayloadBytes.length);
    assert.equal(mcpPayload.sha256, cliPayloadHash);
    assert.equal(mcpPayload.offset, Number(httpPayload.trailers["x-payload-offset"]));
    assert.equal(mcpPayload.bytes_returned, Number(httpPayload.trailers["x-payload-bytes"]));

    await stopChild(mcpChild, "MCP process shutdown", true);
    const mcpStdoutLines = mcpOutput.stdout.split(/\r?\n/);
    if (mcpStdoutLines.at(-1) === "") mcpStdoutLines.pop();
    for (const line of mcpStdoutLines) {
      assert.notEqual(line, "", "MCP stdout must not contain blank protocol lines");
      const message = JSON.parse(line);
      assert.equal(message.jsonrpc, "2.0", "MCP stdout must contain only JSON-RPC messages");
    }
    assert.ok(!mcpOutput.stderr || (!mcpOutput.stderr.includes(marker) && !mcpOutput.stderr.includes(temp) && !mcpOutput.stderr.includes(httpToken)), "MCP diagnostics disclosed test evidence or credentials");
    mcpChild = undefined;
    await stopChild(httpChild, "HTTP process shutdown");
    await eventually(async () => assert.rejects(requestHTTP(`${httpURL}/v1/health`, authorization)), "wait for HTTP listener shutdown");
    const httpStdoutLines = httpOutput.stdout.split(/\r?\n/).filter(line => line !== "");
    assert.deepEqual(httpStdoutLines, [httpURL], "serve stdout must contain exactly one nonempty bound URL line");
    for (const stream of [httpOutput.stdout, httpOutput.stderr]) {
      for (const secret of [httpToken, marker, join(runtime, "token-smith.sock"), join(runtime, "token-smith.sqlite"), lockOwnerToken]) {
        assert.ok(!stream.includes(secret), "HTTP output disclosed credentials, test evidence, or runtime metadata");
      }
    }
    httpChild = undefined;

    assert.equal((await stat(runtime)).mode & 0o777, 0o700);
    assert.equal((await stat(join(runtime, "token-smith.sqlite"))).mode & 0o777, 0o600);
    assert.equal((await stat(join(runtime, "token-smith.sock"))).mode & 0o777, 0o600);
    assert.equal((await stat(join(runtime, "token-smith.lock"))).mode & 0o777, 0o600);
  } finally {
    let cleanupError;
    for (const [child, description, closeInput] of [[mcpChild, "MCP process cleanup", true], [httpChild, "HTTP process cleanup", false]]) {
      try {
        await stopChild(child, description, closeInput);
      } catch (error) {
        cleanupError ??= error;
      }
    }
    try {
      await stopDaemonSafely(runtime, binary, daemonIdentity);
      cleanupConfirmed = cleanupError === undefined;
    } catch (error) {
      cleanupError ??= error;
    }
    if (originalHome === undefined) delete process.env.HOME;
    else process.env.HOME = originalHome;
    delete process.env.PI_TOKEN_SMITH_BIN;
    if (!cleanupConfirmed) {
      throw new Error(`daemon cleanup failed; preserving temporary runtime at ${temp}: ${cleanupError?.message ?? "unknown cleanup error"}`, { cause: cleanupError });
    }
    await rm(temp, { recursive: true, force: true });
    await assert.rejects(stat(temp), /ENOENT/);
  }
});
