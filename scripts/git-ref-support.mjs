import { execFileSync } from "node:child_process";

/**
 * Verify that the ref resolves to a commit before either content-search tool reads it. Callers add their
 * own tool-name prefix to errors.
 */
export function verifyRefResolves(cwd, ref) {
  let verified;
  try {
    verified = execFileSync("git", ["rev-parse", "--verify", "--quiet", `${ref}^{commit}`], { cwd, encoding: "utf8" }).trim();
  } catch {
    verified = "";
  }
  if (verified.length === 0) throw new Error(`ref ${JSON.stringify(ref)} does not resolve to a commit.`);
}
