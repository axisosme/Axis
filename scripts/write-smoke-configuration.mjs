import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { stringify } from "smol-toml";

import { generateAxisSecrets, loadAxisConfiguration, readAxisSecret } from "../dist/configuration.js";

/** Disposable topology for distribution and live-validation harnesses. */
export async function temporaryConfiguration(root, document) {
  await mkdir(root, { recursive: true, mode: 0o700 });
  const filename = join(root, "axis.toml");
  await writeFile(filename, stringify({ schemaVersion: 1, dataDir: "data", router: { port: 8788 }, ...document }), { mode: 0o600 });
  const config = loadAxisConfiguration(filename);
  generateAxisSecrets(config);
  return { config, filename, key: name => readAxisSecret(config, name) };
}
