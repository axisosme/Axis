#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { constants, closeSync, fstatSync, openSync, readFileSync, realpathSync } from "node:fs";
import { homedir } from "node:os";
import { isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";

// Installed as `linear` by deployment. Agent arguments pass through unchanged.
// Always read the CLI's own credential, overriding inherited Run/personal values.
try {
  if (process.env.LINEAR_CLI_LAUNCHER_ACTIVE === "1") throw new Error();
  const path = join(homedir(), ".axis", "linear-cli", "config.json");
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  let config: { executable: string; endpoint: string; credential: string };
  try {
    const stat = fstatSync(fd);
    if (!stat.isFile() || (stat.mode & 0o077) !== 0 || stat.uid !== process.getuid?.() || stat.size > 16_384) throw new Error();
    config = JSON.parse(readFileSync(fd, "utf8"));
  } finally { closeSync(fd); }
  const endpoint = new URL(config.endpoint);
  if (!isAbsolute(config.executable) || realpathSync(config.executable) === realpathSync(fileURLToPath(import.meta.url))
    || !/^axlcli1_[A-Za-z0-9_-]{43}$/u.test(config.credential)
    || endpoint.username || endpoint.password || endpoint.search || endpoint.hash || endpoint.pathname !== "/api/linear/graphql"
    || (endpoint.protocol !== "https:" && !(endpoint.protocol === "http:" && ["127.0.0.1", "[::1]"].includes(endpoint.hostname)))) throw new Error();
  const environment: NodeJS.ProcessEnv = { ...process.env, LINEAR_API_KEY: config.credential, LINEAR_GRAPHQL_ENDPOINT: endpoint.href, LINEAR_EXECUTION_STATUS: "available", LINEAR_CLI_LAUNCHER_ACTIVE: "1" };
  delete environment.LINEAR_EXECUTION_EXPIRES_AT;
  delete environment.LINEAR_EXECUTION_SDK_MODULE;
  const result = spawnSync(config.executable, process.argv.slice(2), { env: environment, stdio: "inherit", shell: false });
  process.exitCode = result.status ?? 1;
} catch {
  process.stderr.write("Axis Linear CLI configuration is missing or invalid. Contact the deployment operator.\n");
  process.exitCode = 1;
}
