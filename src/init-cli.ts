import { initializeAxis } from "./configuration.js";
import { InitError, runInitWizard, type InitOptions } from "./init.js";
import { terminalInitPrompts } from "./init-prompts.js";

export const initUsage = "axis init [--interactive | --non-interactive] [--config <axis.toml>] [--data-dir <directory>] [--env-file <axis.env>]";
const usage = `${initUsage}\nBare init starts interactive setup on a terminal. Explicit --config or --data-dir preserves scripted key initialization.\nInteractive setup requires new files and never overwrites existing keys.\n`;

export async function runInitCli(argv: readonly string[]): Promise<number> {
  if (argv.length === 1 && argv[0] === "--help") { process.stdout.write(usage); return 0; }
  let prompts: ReturnType<typeof terminalInitPrompts> | undefined;
  try {
    const options: { filename?: string; dataDir?: string; envFile?: string } = {};
    let mode: "interactive" | "non-interactive" | undefined;
    for (let i = 0; i < argv.length; i++) {
      const flag = argv[i];
      if (flag === "--interactive" || flag === "--non-interactive") {
        if (mode !== undefined) throw new InitError("Select exactly one init mode.");
        mode = flag === "--interactive" ? "interactive" : "non-interactive";
        continue;
      }
      const key = flag === "--config" ? "filename" : flag === "--data-dir" ? "dataDir" : flag === "--env-file" ? "envFile" : undefined;
      const value = argv[++i];
      if (key === undefined || options[key] !== undefined || value === undefined || value.length === 0 || value.startsWith("--")) throw new InitError("Invalid init arguments. Run axis init --help.");
      options[key] = value;
    }
    const interactive = mode === "interactive" || mode === undefined && options.filename === undefined && options.dataDir === undefined;
    if (options.envFile !== undefined && !interactive) throw new InitError("--env-file requires --interactive.");
    if (interactive) {
      if (!process.stdin.isTTY || !process.stdout.isTTY) throw new InitError("Interactive setup requires a terminal. For scripts and containers use axis init --non-interactive [--config <axis.toml>] [--data-dir <directory>].");
      prompts = terminalInitPrompts();
      await runInitWizard(options satisfies InitOptions, prompts);
    } else {
      initializeAxis(options);
      process.stdout.write("Axis configuration and private secrets initialized.\n");
    }
    return 0;
  } catch (error) {
    process.stderr.write(error instanceof InitError ? `Axis initialization refused: ${error.message}\n` : "Axis initialization refused or interrupted. Created files were preserved; check configuration and private files before manual recovery.\n");
    return 1;
  } finally { prompts?.close(); }
}
