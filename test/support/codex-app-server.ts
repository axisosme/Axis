import { type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";

/**
 * Shared in-process stand-in for `codex app-server`. Records every client
 * message so a test can assert the exact request sequence, and answers
 * `thread/start` / `turn/start` unless a test overrides a method.
 */
export function fakeAppServer(responses: Readonly<Record<string, unknown>> = {}): {
  readonly child: ChildProcess;
  readonly spawnCalls: { readonly file: string; readonly args: readonly string[] }[];
  readonly messages: readonly Record<string, unknown>[];
  notify(method: string, params: Record<string, unknown>): void;
  request(id: number | string, method: string, params: Record<string, unknown>): void;
} {
  const stdin = new PassThrough();
  const stdout = new PassThrough();
  const emitter = new EventEmitter();
  const spawnCalls: { file: string; args: readonly string[] }[] = [];
  const messages: Record<string, unknown>[] = [];
  let buffer = "";
  let turn = 0;
  const child = Object.assign(emitter, {
    stdin,
    stdout,
    stderr: null,
    kill: () => true,
  }) as unknown as ChildProcess;
  stdin.setEncoding("utf8");
  stdin.on("data", (chunk: string) => {
    buffer += chunk;
    for (;;) {
      const newline = buffer.indexOf("\n");
      if (newline < 0) return;
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      const message = JSON.parse(line) as Record<string, unknown>;
      messages.push(message);
      if (typeof message.id !== "number") continue;
      const method = typeof message.method === "string" ? message.method : "";
      const result = responses[method] ?? (method === "thread/start"
        ? { thread: { id: "thread-native-1" } }
        : method === "turn/start"
          ? { turn: { id: `turn-native-${++turn}` } }
          : {});
      queueMicrotask(() => { stdout.write(`${JSON.stringify({ id: message.id, result })}\n`); });
    }
  });
  return {
    child,
    spawnCalls,
    messages,
    notify(method, params) { stdout.write(`${JSON.stringify({ method, params })}\n`); },
    request(id, method, params) { stdout.write(`${JSON.stringify({ id, method, params })}\n`); },
  };
}
