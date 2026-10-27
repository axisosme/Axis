import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const sourceRoot = path.resolve(fileURLToPath(new URL("../", import.meta.url)));
const publishedRoot = path.resolve(fileURLToPath(new URL("../../", import.meta.url)));

// Source builds live at <root>/src while published builds live at <root>/dist/src.
// Resolve only bundled contract resources; callers never supply filesystem paths.
export const runtimePackageRoot = existsSync(path.join(sourceRoot, "conformance")) ? sourceRoot : publishedRoot;

export function fromRuntimePackageRoot(...segments: string[]): string {
  return path.join(runtimePackageRoot, ...segments);
}
