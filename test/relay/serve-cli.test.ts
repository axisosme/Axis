import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, mock, test } from "node:test";

let startupSignal: NodeJS.Signals | undefined;
let closes = 0;
const mocked = mock.module(new URL("../../src/serve.ts", import.meta.url).href, { namedExports: {
  startServe: async (_config: unknown, options: { signal: AbortSignal }) => {
    if (startupSignal !== undefined) {
      process.emit(startupSignal);
      options.signal.throwIfAborted();
    }
    return { close: async () => { closes += 1; } };
  },
} });
after(() => mocked.restore());
const { runServeCli } = await import("../../src/serve-cli.js");

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  for (const duringStartup of [false, true]) {
    test(`${signal} ${duringStartup ? "during startup" : "after readiness"} exits cleanly and removes signal handlers`, async context => {
      const root = mkdtempSync(join(tmpdir(), "axis-serve-cli-"));
      context.after(() => rmSync(root, { recursive: true, force: true }));
      const filename = join(root, "axis.toml");
      writeFileSync(filename, 'schemaVersion = 1\ndataDir = "data"\n[serve]\norganizationId = "example-organization"\n');
      const counts = [process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")];
      startupSignal = duringStartup ? signal : undefined;
      closes = 0;
      let stdout = "", stderr = "";
      context.mock.method(process.stdout, "write", (text: string) => {
        stdout += text;
        if (text === "Axis serve ready\n") queueMicrotask(() => process.emit(signal));
        return true;
      });
      context.mock.method(process.stderr, "write", (text: string) => { stderr += text; return true; });
      assert.equal(await runServeCli(["--config", filename]), 0);
      assert.equal(closes, duringStartup ? 0 : 1);
      assert.equal(stdout.includes("Axis serve ready"), !duringStartup);
      assert.equal(stderr, "");
      assert.deepEqual([process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")], counts);
    });
  }
}
