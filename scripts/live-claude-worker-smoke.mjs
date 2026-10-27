import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { readFile, realpath, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { ClaudeCodeExecutor } from "../dist/executor/claude-code.js";
import { startClaudeWorker } from "../dist/worker/claude-runtime.js";
import { deriveWorkerConfiguration } from "../dist/configuration.js";
import { temporaryConfiguration } from "./write-smoke-configuration.mjs";
import { WORKER_COMMAND_VERSION } from "../dist/shared/router-worker.js";
import { CODEX_EXECUTOR_CONTRACT_VERSION } from "../dist/shared/worker-executor.js";

if (!process.argv.includes("--live")) {
  process.stdout.write("Claude Worker live smoke skipped; pass --live --executable <absolute> explicitly.\n");
  process.exit(0);
}

const configuredExecutable = process.argv[process.argv.indexOf("--executable") + 1];
assert.ok(configuredExecutable !== undefined && configuredExecutable.startsWith("/"), "--executable must name the real absolute Claude Code executable");
const executable = await realpath(configuredExecutable);
const temporary = await mkdtemp(join(tmpdir(), "axis-claude-live-"));
const workspaceRoot = join(temporary, "workspace");
let workerToken, routerToken;
const instruction = "Reply with exactly OK. Do not use tools or access files.";
// a turn that is still generating or running its tool when the cancel lands.
const longInstruction = "Use the Bash tool to run exactly this command and wait for it to finish: for i in $(seq 1 30); do echo $i; sleep 1; done. Then reply with exactly COUNTED.";
// a word the session is told before a Worker restart and asked for after it.
const rememberedWord = `axis${randomBytes(4).toString("hex")}`;
const rememberInstruction = `Remember this word for later in this conversation: ${rememberedWord}. Do not use tools or access files. Reply with exactly OK.`;
const recallInstruction = "Use a tool to write the word I asked you to remember earlier in this conversation, and nothing else, to a file named recalled.txt in the current directory. Then reply with exactly DONE.";
// a Worker `steer` lands while this turn's 15-second Bash call runs.
const steeredTurnInstruction = "Use the Bash tool to run exactly this command in the foreground (never as a background task) and wait for it to finish: sleep 15 && echo done. Then report the command's output in one short sentence.";
const workerSteerInstruction = "When you report the output, also end your reply with the word MANGO.";
const events = [];
let runtime;
let router;

async function unusedPort() {
  const server = createNetServer();
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const address = server.address();
  assert.ok(address !== null && typeof address !== "string");
  await new Promise((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
  return address.port;
}

function sendJson(response, status, value) {
  const body = JSON.stringify(value);
  response.writeHead(status, { "content-type": "application/json; charset=utf-8", "content-length": Buffer.byteLength(body), "cache-control": "no-store" });
  response.end(body);
}

try {
  await mkdir(workspaceRoot);
  router = createServer((request, response) => {
    void (async () => {
      assert.equal(request.method, "POST");
      assert.equal(request.url, "/v1/router/run-events");
      assert.equal(request.headers.authorization, `Bearer ${routerToken}`);
      let body = "";
      for await (const chunk of request) body += chunk.toString();
      const payload = JSON.parse(body);
      assert.deepEqual(Object.keys(payload).sort(), ["envelope", "recordedAt"]);
      events.push(payload.envelope);
      sendJson(response, 202, { accepted: true });
    })().catch(() => sendJson(response, 400, { accepted: false }));
  });
  await new Promise((resolve, reject) => { router.once("error", reject); router.listen(0, "127.0.0.1", resolve); });
  const routerAddress = router.address();
  assert.ok(routerAddress !== null && typeof routerAddress !== "string");
  const workerPort = await unusedPort();
  const deployment = await temporaryConfiguration(temporary, { nodes: [{ id: "claude-live-node", port: await unusedPort(), eventPort: routerAddress.port,
    workers: [{ id: "claude-live-worker", kind: "claude-code", executorId: "claude-live-executor", port: workerPort, claudeExecutable: executable,
      workspaces: [{ organizationId: "claude-live-org", workspaceRoot }] }] }] });
  const parsedWorker = deriveWorkerConfiguration(deployment.config);
  const workerDatabase = parsedWorker.database;
  workerToken = parsedWorker.token; routerToken = parsedWorker.routerToken;
  const workerEnvironment = process.env;
  runtime = await startClaudeWorker(parsedWorker, workerEnvironment);

  const correlationId = randomUUID();
  const taskId = randomUUID();
  // the identity this live Worker is configured with (runtime profile defaults to its executor id).
  const executionTarget = { identity: { nodeId: "claude-live-node", workerId: "claude-live-worker", executorKind: "claude-code", runtimeProfile: { ref: "claude-live-executor", version: "v1" } }, sessionBindingExecutorId: "claude-live-executor" };

  function startIdentity() {
    const runId = randomUUID();
    const attemptId = randomUUID();
    const sessionBindingId = randomUUID();
    return {
      runId,
      start: {
        contractVersion: "1.0.0", messageId: randomUUID(), correlationId, operationId: randomUUID(), causationMessageId: randomUUID(), occurredAt: new Date().toISOString(), producer: "router", consumer: "worker", envelopeType: "run.start",
        identity: {
          contractVersion: "1.0.0",
          task: { contractVersion: "1.0.0", taskId, correlationId, sourceRef: { system: "linear", organizationId: "claude-live-org", issueId: "claude-live-issue" } },
          attempt: { contractVersion: "1.0.0", attemptId, taskId, correlationId },
          run: { contractVersion: "1.0.0", runId, taskId, attemptId, sessionBindingId, correlationId, state: "pending" },
          sessionBinding: { contractVersion: "1.0.0", sessionBindingId, taskId, attemptId, runId, correlationId, workerId: "claude-live-worker", executorId: "claude-live-executor" },
        },
      },
    };
  }

  // two turns on the same native session -- a `start` and a `continue`
  // naming it as `continuation.priorRunId` -- to prove the streaming-input
  // launch (stdin held open through a turn, `--resume` under it) actually
  // completes a real turn twice against the real CLI, not just once.
  async function postCommand(payload) {
    const response = await fetch(`http://127.0.0.1:${workerPort}/v1/worker/commands`, { method: "POST", headers: { authorization: `Bearer ${workerToken}`, "content-type": "application/json" }, body: JSON.stringify(payload) });
    const body = await response.json();
    assert.equal(response.status, 202, `${payload.command} was refused: ${JSON.stringify(body)}`);
    return body;
  }

  async function submitAndAwaitTerminal(commandId, payload, runId, { expectedTerminal = "run.succeeded", whileRunning } = {}) {
    const acceptance = await postCommand(payload);
    assert.equal(acceptance.contractVersion, WORKER_COMMAND_VERSION);
    assert.deepEqual(acceptance.identity, executionTarget.identity);
    assert.equal(acceptance.commandId, commandId);
    assert.equal(acceptance.status, "accepted");
    assert.deepEqual(Object.keys(acceptance).sort(), ["commandId", "contractVersion", "identity", "runAccepted", "status"]);

    const deadline = Date.now() + 120_000;
    const forThisRun = () => events.filter((event) => event.run.runId === runId);
    if (whileRunning !== undefined) await whileRunning(forThisRun);
    while (!forThisRun().some((event) => event.envelopeType === "run.succeeded" || event.envelopeType === "run.failed")) {
      assert.ok(Date.now() < deadline, "Claude Worker live smoke did not reach a terminal event");
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    const runEvents = forThisRun();
    assert.equal(runEvents.filter((event) => event.envelopeType === "run.accepted").length, 1);
    assert.deepEqual(runEvents.find((event) => event.envelopeType === "run.accepted"), acceptance.runAccepted);
    assert.ok(runEvents.filter((event) => event.envelopeType === "run.progressed").length <= 1);
    const terminal = runEvents.at(-1);
    assert.equal(terminal.envelopeType, expectedTerminal);
    if (expectedTerminal === "run.succeeded") assert.equal(terminal.resultRef, `axis-evidence://${runId}/claude-code-terminal`);
    return { acceptance, runEvents };
  }

  const first = startIdentity();
  const firstCommandId = randomUUID();
  const { acceptance: firstAcceptance, runEvents: firstEvents } = await submitAndAwaitTerminal(
    firstCommandId,
    { contractVersion: WORKER_COMMAND_VERSION, commandId: firstCommandId, command: "start", start: first.start, instruction, executionTarget, workspacePolicyRef: null },
    first.runId,
  );
  assert.equal(runtime.nativeSessionLaunchCount(), 1);

  const second = startIdentity();
  const secondCommandId = randomUUID();
  const { acceptance: secondAcceptance, runEvents: secondEvents } = await submitAndAwaitTerminal(
    secondCommandId,
    { contractVersion: WORKER_COMMAND_VERSION, commandId: secondCommandId, command: "continue", start: second.start, instruction, continuation: { priorRunId: first.runId }, executionTarget, workspacePolicyRef: null },
    second.runId,
  );
  // A continuation resumes the same native Claude Code session (`--resume`); it
  // never spawns a fresh `--session-id` launch, so this count -- incremented
  // only on `operation === "start"` -- stays at one turn's worth after two.
  assert.equal(runtime.nativeSessionLaunchCount(), 1);

  // a third turn on the same session is cancelled mid-turn, through the
  // Worker's own `cancel` command, and a fourth continuation
  // then succeeds on that same session.
  const third = startIdentity();
  const thirdCommandId = randomUUID();
  let cancelResponse;
  let cancelledAt;
  const { acceptance: thirdAcceptance, runEvents: thirdEvents } = await submitAndAwaitTerminal(
    thirdCommandId,
    { contractVersion: WORKER_COMMAND_VERSION, commandId: thirdCommandId, command: "continue", start: third.start, instruction: longInstruction, continuation: { priorRunId: second.runId }, executionTarget, workspacePolicyRef: null },
    third.runId,
    {
      expectedTerminal: "run.failed",
      whileRunning: async (forThisRun) => {
        // Cancel two seconds after the turn's first progress, inside its first
        // generation or tool call. Waiting longer is not safe: Claude Code may run
        // the loop as a background task that returns at once, and the turn can
        // then finish within about eight seconds, so a later cancel races a
        // completed result (which correctly stays succeeded, and fails the cancel).
        const progressDeadline = Date.now() + 60_000;
        while (!forThisRun().some((event) => event.envelopeType === "run.progressed")) {
          assert.ok(Date.now() < progressDeadline, "the long turn never progressed");
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
        await new Promise((resolve) => setTimeout(resolve, 2_000));
        assert.equal(forThisRun().some((event) => event.envelopeType === "run.succeeded" || event.envelopeType === "run.failed"), false, "the long turn ended before it could be cancelled");
        const cancelCommandId = randomUUID();
        cancelResponse = await postCommand({ contractVersion: WORKER_COMMAND_VERSION, commandId: cancelCommandId, command: "cancel", target: { runId: third.runId } });
        cancelledAt = Date.now();
        assert.equal(cancelResponse.commandId, cancelCommandId);
        assert.equal(cancelResponse.status, "interrupt_requested");
        assert.equal(cancelResponse.runId, third.runId);
      },
    },
  );
  // The interrupt, not the turn's own completion, ended it: only an interrupted
  // turn projects `execution_cancelled` (asserted below). This bound only
  // records that the terminal followed the cancel promptly.
  const cancelToTerminalMs = Date.now() - cancelledAt;
  assert.ok(cancelToTerminalMs < 15_000, `the cancelled turn took ${cancelToTerminalMs}ms to reach its terminal`);
  const cancelledTerminal = thirdEvents.at(-1);
  assert.equal(cancelledTerminal.error.code, "execution_cancelled");
  assert.equal(cancelledTerminal.run.state, "failed");

  const fourth = startIdentity();
  const fourthCommandId = randomUUID();
  const { acceptance: fourthAcceptance, runEvents: fourthEvents } = await submitAndAwaitTerminal(
    fourthCommandId,
    { contractVersion: WORKER_COMMAND_VERSION, commandId: fourthCommandId, command: "continue", start: fourth.start, instruction, continuation: { priorRunId: third.runId }, executionTarget, workspacePolicyRef: null },
    fourth.runId,
  );
  assert.equal(runtime.nativeSessionLaunchCount(), 1);

  // the session learns a word, the Worker is shut down and started
  // again on the same database, and a continuation of that Run -- one the new
  // process never saw -- is accepted, resumes the same native session, and the
  // session still knows the word.
  const fifth = startIdentity();
  const fifthCommandId = randomUUID();
  const { acceptance: fifthAcceptance, runEvents: fifthEvents } = await submitAndAwaitTerminal(
    fifthCommandId,
    { contractVersion: WORKER_COMMAND_VERSION, commandId: fifthCommandId, command: "continue", start: fifth.start, instruction: rememberInstruction, continuation: { priorRunId: fourth.runId }, executionTarget, workspacePolicyRef: null },
    fifth.runId,
  );
  const storePath = parsedWorker.nativeSessionStorePath;
  const storeDeadline = Date.now() + 10_000;
  let store;
  for (;;) {
    try { store = JSON.parse(await readFile(storePath, "utf8")); break; } catch {}
    assert.ok(Date.now() < storeDeadline, "the Claude Worker never wrote its native session store");
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  // The Worker canonicalizes its workspace roots, so the recorded directory is the real path.
  const canonicalWorkspaceRoot = await realpath(workspaceRoot);
  assert.deepEqual(Object.values(store), [{ workingDirectory: canonicalWorkspaceRoot }], "one session, recorded with the working directory it ran in");
  await runtime.close();
  runtime = undefined;
  runtime = await startClaudeWorker(parsedWorker, workerEnvironment);
  const sixth = startIdentity();
  const sixthCommandId = randomUUID();
  const { acceptance: sixthAcceptance, runEvents: sixthEvents } = await submitAndAwaitTerminal(
    sixthCommandId,
    { contractVersion: WORKER_COMMAND_VERSION, commandId: sixthCommandId, command: "continue", start: sixth.start, instruction: recallInstruction, continuation: { priorRunId: fifth.runId }, executionTarget, workspacePolicyRef: null },
    sixth.runId,
  );
  assert.equal(runtime.nativeSessionLaunchCount(), 0, "the restarted Worker resumed the session rather than starting one");
  const recalled = (await readFile(join(workspaceRoot, "recalled.txt"), "utf8")).trim();
  assert.equal(recalled, rememberedWord, "the resumed session kept the context from before the restart");

  // the Worker's own `steer` command, mid-Bash, on the restarted
  // Worker: the running turn takes the instruction (`steered`, not the
  // `interrupt_requested` fallback) and still ends `run.succeeded`.
  const seventh = startIdentity();
  const seventhCommandId = randomUUID();
  let steerResponse;
  const { acceptance: seventhAcceptance, runEvents: seventhEvents } = await submitAndAwaitTerminal(
    seventhCommandId,
    { contractVersion: WORKER_COMMAND_VERSION, commandId: seventhCommandId, command: "continue", start: seventh.start, instruction: steeredTurnInstruction, continuation: { priorRunId: sixth.runId }, executionTarget, workspacePolicyRef: null },
    seventh.runId,
    {
      whileRunning: async (forThisRun) => {
        // As for the cancel above: two seconds after the first progress is
        // inside the Bash call, before the model could finish on its own.
        const progressDeadline = Date.now() + 60_000;
        while (!forThisRun().some((event) => event.envelopeType === "run.progressed")) {
          assert.ok(Date.now() < progressDeadline, "the steered turn never progressed");
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
        await new Promise((resolve) => setTimeout(resolve, 2_000));
        assert.equal(forThisRun().some((event) => event.envelopeType === "run.succeeded" || event.envelopeType === "run.failed"), false, "the steered turn ended before the steer; the model may have backgrounded the sleep");
        const steerCommandId = randomUUID();
        steerResponse = await postCommand({ contractVersion: WORKER_COMMAND_VERSION, commandId: steerCommandId, command: "steer", target: { runId: seventh.runId }, instruction: workerSteerInstruction });
        assert.deepEqual(steerResponse, { contractVersion: WORKER_COMMAND_VERSION, commandId: steerCommandId, status: "steered", runId: seventh.runId });
      },
    },
  );

  // the real CLI's answer to `--resume` of a session Axis recorded but
  // whose transcript does not exist is a definite refusal, not ambiguity.
  const missingSessionId = randomUUID();
  const missingStorePath = join(temporary, "missing-sessions.json");
  await writeFile(missingStorePath, JSON.stringify({ [missingSessionId]: { workingDirectory: canonicalWorkspaceRoot } }), { mode: 0o600 });
  const missingExecutor = new ClaudeCodeExecutor({ command: executable, environment: { HOME: process.env.HOME, USER: process.env.USER, PATH: process.env.PATH, TMPDIR: process.env.TMPDIR }, nativeSessionStorePath: missingStorePath });
  let missingResult;
  try {
    const missingBinding = { runtimeKind: "claude-code", sessionId: missingSessionId };
    assert.equal((await missingExecutor.reattestTerminal({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: randomUUID(), binding: missingBinding, expectedOutcome: "succeeded" }))?.status, "attested");
    missingResult = await missingExecutor.continue({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: randomUUID(), workingDirectory: canonicalWorkspaceRoot, instruction, binding: missingBinding });
  } finally {
    missingExecutor.close();
  }
  assert.equal(missingResult.status, "failed");
  assert.equal(missingResult.error.code, "executor.launch_failed");

  // Drive native soft steer directly and tap the child stream for message UUIDs, replay echoes, init/result
  // counts, final response text, and exit evidence. The response text is checked privately and never
  // printed.
  const steerSentence = "最后以单词 PINEAPPLE 结束回复";
  const toolInstruction = "Use the Bash tool to run exactly this command in the foreground (never as a background task) and wait for it to finish: sleep 15 && echo done. Then report the command's output in one short sentence.";
  const storyInstruction = "Write a story of about 250 words about a lighthouse keeper. Do not use tools or access files.";
  const taps = [];
  const steerExecutor = new ClaudeCodeExecutor({
    command: executable,
    environment: { HOME: process.env.HOME, USER: process.env.USER, PATH: process.env.PATH, TMPDIR: process.env.TMPDIR },
    spawnProcess: (file, args, options) => {
      const child = spawn(file, [...args], { ...options, stdio: [...options.stdio] });
      if (!args.includes("-p")) return child;
      const tap = { userUuids: [], inits: 0, results: 0, echoedSteers: 0, textDeltas: 0, lastResultText: undefined, exit: undefined };
      taps.push(tap);
      const write = child.stdin.write.bind(child.stdin);
      child.stdin.write = (chunk, ...rest) => {
        for (const line of String(chunk).split("\n").filter((text) => text.length > 0)) {
          try { const message = JSON.parse(line); if (message.type === "user") tap.userUuids.push(message.uuid); } catch {}
        }
        return write(chunk, ...rest);
      };
      let pending = "";
      child.stdout.on("data", (chunk) => {
        pending += String(chunk);
        for (let newline = pending.indexOf("\n"); newline >= 0; newline = pending.indexOf("\n")) {
          const line = pending.slice(0, newline);
          pending = pending.slice(newline + 1);
          let message;
          try { message = JSON.parse(line); } catch { continue; }
          if (message.type === "system" && message.subtype === "init") tap.inits += 1;
          if (message.type === "result") { tap.results += 1; tap.lastResultText = typeof message.result === "string" ? message.result : undefined; }
          if (message.type === "user" && message.isReplay === true && tap.userUuids.indexOf(message.uuid) > 0) tap.echoedSteers += 1;
          if (message.type === "stream_event" && message.event?.delta?.type === "text_delta") tap.textDeltas += 1;
        }
      });
      child.once("exit", (code, signal) => { tap.exit = { code, signal }; });
      return child;
    },
  });
  async function startObserved(instructionText) {
    const accepted = await steerExecutor.start({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: randomUUID(), workingDirectory: canonicalWorkspaceRoot, instruction: instructionText });
    assert.equal(accepted.status, "accepted", `the steer scenario start was refused: ${JSON.stringify(accepted.error ?? null)}`);
    const tap = taps.at(-1);
    const outcomes = [];
    let done = false;
    const finished = (async () => {
      try { for await (const event of steerExecutor.observe(accepted.binding)) outcomes.push(event.outcome); return "terminal"; }
      catch { return "ambiguous"; }
      finally { done = true; }
    })();
    return { binding: accepted.binding, tap, outcomes, finished, isDone: () => done };
  }
  async function within(promise, ms, label) {
    let timer;
    try { return await Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(label)), ms); })]); }
    finally { clearTimeout(timer); }
  }
  const pineapple = (tap) => typeof tap.lastResultText === "string" && tap.lastResultText.includes("PINEAPPLE");
  const steerEvidence = {};
  try {
    // (a) Tool-time: the steer lands while `sleep 15` runs, is consumed at the
    // tool boundary, and the one turn ends with one result that follows it.
    const toolTime = await startObserved(toolInstruction);
    await new Promise((resolve) => setTimeout(resolve, 8_000));
    assert.equal(toolTime.isDone(), false, "the tool-time turn ended before the steer; the model may have backgrounded the sleep");
    const toolSteer = await steerExecutor.steer({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: randomUUID(), binding: toolTime.binding, instruction: steerSentence });
    assert.equal(await within(toolTime.finished, 120_000, "the tool-time steer turn did not finish"), "terminal");
    steerEvidence.toolTime = { steer: toolSteer.status, terminal: toolTime.outcomes.at(-1), inits: toolTime.tap.inits, results: toolTime.tap.results, echoedSteers: toolTime.tap.echoedSteers, pineapple: pineapple(toolTime.tap), exit: toolTime.tap.exit };
    process.stdout.write(`Claude steer (a) tool-time: ${JSON.stringify(steerEvidence.toolTime)}\n`);
    assert.equal(toolSteer.status, "steered");
    assert.equal(toolTime.outcomes.at(-1), "succeeded");
    assert.equal(toolTime.tap.results, 1, "a tool-time steer joins the running turn: exactly one result");
    assert.equal(toolTime.tap.echoedSteers, 1, "the CLI echoed the steer back");
    assert.ok(pineapple(toolTime.tap), "the final result does not end with PINEAPPLE. The model may have refused the tool-time steer as possible prompt injection; rerun before treating this as an adapter defect");

    // (b) Generation-time: the steer lands while the model writes its final
    // text; that turn ends first, then the same process runs a further turn
    // (a second init and result) that follows it. One Run, one terminal.
    const generation = await startObserved(storyInstruction);
    const deltaDeadline = Date.now() + 60_000;
    while (generation.tap.textDeltas === 0) {
      assert.ok(Date.now() < deltaDeadline && !generation.isDone(), "the story turn never started writing text");
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    const generationSteer = await steerExecutor.steer({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: randomUUID(), binding: generation.binding, instruction: steerSentence });
    assert.equal(await within(generation.finished, 180_000, "the generation-time steer turn did not finish"), "terminal");
    steerEvidence.generation = { steer: generationSteer.status, terminal: generation.outcomes.at(-1), terminals: generation.outcomes.filter((outcome) => outcome === "succeeded" || outcome === "failed" || outcome === "cancelled").length, inits: generation.tap.inits, results: generation.tap.results, echoedSteers: generation.tap.echoedSteers, pineapple: pineapple(generation.tap), exit: generation.tap.exit };
    process.stdout.write(`Claude steer (b) generation-time: ${JSON.stringify(steerEvidence.generation)}\n`);
    assert.equal(generationSteer.status, "steered");
    assert.equal(generation.outcomes.at(-1), "succeeded");
    assert.equal(steerEvidence.generation.terminals, 1);
    assert.equal(generation.tap.inits, 2, "the steer ran in a further turn of the same process");
    assert.equal(generation.tap.results, 2);
    assert.ok(pineapple(generation.tap), "the further turn's result does not end with PINEAPPLE");

    // (c) Queued steer, then stop: the cancel meets a steer the CLI still
    // holds, so the interrupted turn is the last one -- `cancelled`, and the
    // process is stopped rather than left to run the steer.
    const queued = await startObserved(toolInstruction);
    await new Promise((resolve) => setTimeout(resolve, 8_000));
    assert.equal(queued.isDone(), false, "the queued-steer turn ended before the steer; the model may have backgrounded the sleep");
    const queuedSteer = await steerExecutor.steer({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: randomUUID(), binding: queued.binding, instruction: steerSentence });
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    const queuedCancel = await steerExecutor.cancel({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: randomUUID(), binding: queued.binding });
    assert.equal(await within(queued.finished, 60_000, "the stopped turn did not finish"), "terminal");
    const exitDeadline = Date.now() + 10_000;
    while (queued.tap.exit === undefined) {
      assert.ok(Date.now() < exitDeadline, "the stopped turn's process never exited");
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    steerEvidence.queuedStop = { steer: queuedSteer.status, cancel: queuedCancel.status, terminal: queued.outcomes.at(-1), inits: queued.tap.inits, results: queued.tap.results, echoedSteers: queued.tap.echoedSteers, exit: queued.tap.exit };
    process.stdout.write(`Claude steer (c) queued steer then stop: ${JSON.stringify(steerEvidence.queuedStop)}\n`);
    assert.equal(queuedSteer.status, "steered");
    assert.equal(queuedCancel.status, "interrupt_requested");
    assert.equal(queued.outcomes.at(-1), "cancelled");
  } finally {
    steerExecutor.close();
  }
  const safeSteerEvidence = JSON.stringify(steerEvidence);
  for (const sensitive of [steerSentence, toolInstruction, storyInstruction, "PINEAPPLE", executable, workspaceRoot, temporary, "session_id", "sessionId"]) assert.equal(safeSteerEvidence.includes(sensitive), false);

  const baseEventKeys = ["attempt", "causationMessageId", "consumer", "contractVersion", "correlationId", "envelopeType", "messageId", "occurredAt", "operationId", "producer", "run", "task"];
  for (const event of events) {
    const variantKeys = event.envelopeType === "run.progressed" ? ["progress"] : event.envelopeType === "run.succeeded" ? ["resultRef"] : event.envelopeType === "run.failed" ? ["error"] : [];
    assert.deepEqual(Object.keys(event).sort(), [...baseEventKeys, ...variantKeys].sort());
  }
  const safeEvidence = JSON.stringify({ firstAcceptance, secondAcceptance, firstEvents, secondEvents, thirdAcceptance, thirdEvents, cancelResponse, fourthAcceptance, fourthEvents, fifthAcceptance, fifthEvents, sixthAcceptance, sixthEvents, seventhAcceptance, seventhEvents, steerResponse });
  for (const sensitive of [instruction, longInstruction, rememberInstruction, recallInstruction, rememberedWord, steeredTurnInstruction, workerSteerInstruction, "MANGO", "OK", "COUNTED", "DONE", executable, workspaceRoot, temporary, workerToken, routerToken, "session_id", "sessionId", "authMethod", "accessToken", "apiKey"]) assert.equal(safeEvidence.includes(sensitive), false);
  process.stdout.write(`Claude Worker cancel-to-terminal: ${cancelToTerminalMs}ms\n`);
  process.stdout.write(`Claude Worker cancel response: ${JSON.stringify(cancelResponse)}\n`);
  process.stdout.write(`Claude Worker cancelled terminal: ${JSON.stringify({ envelopeType: cancelledTerminal.envelopeType, runState: cancelledTerminal.run.state, runError: cancelledTerminal.run.error, error: cancelledTerminal.error })}\n`);
  process.stdout.write(`Claude Worker turn envelopes: ${JSON.stringify([firstEvents, secondEvents, thirdEvents, fourthEvents, fifthEvents, sixthEvents, seventhEvents].map((runEvents) => runEvents.map((event) => event.envelopeType)))}\n`);
  process.stdout.write(`Claude Worker restart: continuation after restart ${sixthEvents.at(-1).envelopeType}, native launches in the restarted Worker ${runtime.nativeSessionLaunchCount()}, remembered word recalled ${recalled === rememberedWord}\n`);
  process.stdout.write(`Claude Worker steer command: ${JSON.stringify({ status: steerResponse.status, terminal: seventhEvents.at(-1).envelopeType })}\n`);
  process.stdout.write(`Claude missing-transcript resume: ${JSON.stringify({ status: missingResult.status, code: missingResult.error.code })}\n`);
  process.stdout.write("Claude Worker authenticated live smoke passed seven turns on one session (start, continue, a continue cancelled mid-turn, continue, a continue across a Worker restart that kept its context, then a continue steered mid-Bash through the Worker steer command) and a definite refusal for a missing transcript, with redacted fixed evidence.\n");
  process.stdout.write("Claude executor live steer passed: a tool-time steer joined its turn, a generation-time steer ran in a further turn of the same process, and a stop with a steer queued ended cancelled with the process stopped.\n");
} finally {
  if (runtime !== undefined) await runtime.close();
  if (router !== undefined) await new Promise((resolve) => router.close(() => resolve()));
  await rm(temporary, { recursive: true, force: true });
}
