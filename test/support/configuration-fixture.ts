import { createHash } from "node:crypto";
import { mkdir, realpath, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { stringify } from "smol-toml";

import { generateAxisSecrets, loadAxisConfiguration, parseAxisConfiguration } from "../../src/configuration.js";

export const fixtureKey = (name: string): string => createHash("sha256").update(name).digest("base64");
export function configurationDocument(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return { schemaVersion: 1, dataDir: "data", relay: { port: 8787 }, router: { port: 8788 },
    nodes: [{ id: "node-one", port: 9080, eventPort: 9081, workers: [{ id: "worker-one", kind: "codex", port: 9082,
      workspaces: [{ organizationId: "org-one", workspaceRoot: "workspace" }] }] }], ...overrides };
}
export function configurationFixture(overrides: Record<string, unknown> = {}, filename = "/tmp/axis-fixture/axis.toml") {
  return parseAxisConfiguration(stringify(JSON.parse(JSON.stringify(configurationDocument(overrides)))), filename);
}
export async function writeConfigurationFixture(root: string, overrides: Record<string, unknown> = {}) {
  await mkdir(root, { recursive: true, mode: 0o700 });
  root = await realpath(root);
  const filename = join(root, "axis.toml");
  await writeFile(filename, stringify(JSON.parse(JSON.stringify(configurationDocument(overrides)))), { mode: 0o600 });
  const configuration = loadAxisConfiguration(filename);
  generateAxisSecrets(configuration);
  return configuration;
}
