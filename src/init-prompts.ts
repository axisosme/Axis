import { Writable } from "node:stream";
import { createInterface } from "node:readline/promises";

export interface InitPrompts {
  readonly signal?: AbortSignal;
  ask(label: string, options?: { readonly secret?: boolean }): Promise<string>;
  write(message: string): void;
  close(): void;
}

/** Secret input never reaches the terminal, including readline's editing redraws. */
export function terminalInitPrompts(input: NodeJS.ReadableStream = process.stdin, destination: NodeJS.WritableStream = process.stdout): InitPrompts {
  let hidden = false;
  const output = new Writable({ write(chunk, _encoding, done) {
    if (!hidden) destination.write(chunk);
    done();
  } });
  const terminal = createInterface({ input, output, terminal: true, historySize: 0 });
  const cancelled = new AbortController();
  const cancel = (): void => { cancelled.abort(); terminal.close(); };
  terminal.on("SIGINT", cancel);
  terminal.once("close", () => cancelled.abort());
  process.once("SIGTERM", cancel);
  process.once("SIGINT", cancel);
  return {
    signal: cancelled.signal,
    // Ordinary labels are readline's own prompt, so its line redraws keep them on screen; a secret label is written
    // once, outside readline, because every redraw of the hidden line is suppressed.
    async ask(label, options = {}) {
      const secret = options.secret === true;
      if (secret) destination.write(`${label} [hidden]: `);
      hidden = secret;
      try { return await terminal.question(secret ? "" : `${label}: `, { signal: cancelled.signal }); }
      finally { hidden = false; if (secret) destination.write("\n"); }
    },
    write: message => { destination.write(`${message}\n`); },
    close() { process.off("SIGTERM", cancel); process.off("SIGINT", cancel); terminal.close(); output.end(); },
  };
}
