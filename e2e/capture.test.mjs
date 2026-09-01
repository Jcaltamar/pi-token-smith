import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, realpath, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
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

test("captures a synthetic Pi session across the extension, daemon, SQLite, FTS5, and CLI", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-token-smith-e2e-"));
  const home = join(temp, "home");
  const binary = join(temp, "pi-token-smith");
  const runtime = join(home, ".pi", "agent", "pi-token-smith");
  const originalHome = process.env.HOME;
  let daemonIdentity;
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

    assert.equal((await stat(runtime)).mode & 0o777, 0o700);
    assert.equal((await stat(join(runtime, "token-smith.sqlite"))).mode & 0o777, 0o600);
    assert.equal((await stat(join(runtime, "token-smith.sock"))).mode & 0o777, 0o600);
    assert.equal((await stat(join(runtime, "token-smith.lock"))).mode & 0o777, 0o600);
  } finally {
    let cleanupError;
    try {
      await stopDaemonSafely(runtime, binary, daemonIdentity);
      cleanupConfirmed = true;
    } catch (error) {
      cleanupError = error;
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
