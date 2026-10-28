import assert from "node:assert/strict";
import { lstatSync, readFileSync, readdirSync, readlinkSync, realpathSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { runAxisCli } from "../../src/axis-cli.js";

const root = fileURLToPath(new URL("../../", import.meta.url));
const skill = join(root, ".agents/skills/axis-deploy");

function markdownFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    return entry.isDirectory() ? markdownFiles(path) : path.endsWith(".md") ? [path] : [];
  });
}

test("deployment skill discovery paths resolve to the same instructions and references", () => {
  const alias = join(root, ".claude/skills/axis-deploy");
  assert.ok(lstatSync(alias).isSymbolicLink());
  assert.equal(readlinkSync(alias), "../../.agents/skills/axis-deploy");
  assert.equal(realpathSync(alias), realpathSync(skill));
  for (const path of markdownFiles(skill)) {
    const relative = path.slice(skill.length + 1);
    assert.equal(realpathSync(join(alias, relative)), realpathSync(path));
  }
  for (const entry of ["README.md", "AGENTS.md"]) {
    assert.ok(readFileSync(join(root, entry), "utf8").includes(".agents/skills/axis-deploy/SKILL.md"));
  }
});

test("deployment skill Axis commands and flags exist in current CLI help", () => {
  let help = "";
  assert.equal(runAxisCli(["--help"], { stdout: (text) => { help += text; }, stderr: assert.fail }), 0);
  const usage = help.split("\n").filter((line) => /^  axis /u.test(line));
  assert.ok(usage.length > 0);
  let checked = 0;
  for (const filename of markdownFiles(skill)) {
    const content = readFileSync(filename, "utf8");
    const examples = [
      ...Array.from(content.matchAll(/`([^`\n]+)`/gu), (match) => match[1]!),
      ...Array.from(content.matchAll(/```sh\n([\s\S]*?)```/gu), (match) => match[1]!)
        .flatMap((block) => block.split("\n")),
    ];
    for (const example of examples) {
      const invocation = example.trim().replace(/^node(?:\s+--\S+)*\s+\.\/dist\/bin\/axis\.js(?=\s|$)/u, "axis");
      if (!/^axis\s/u.test(invocation)) continue;
      checked += 1;
      if (invocation === "axis --help") continue; // Executed above; help does not list itself.
      const tokens = invocation.split(/\s+/u).slice(1);
      const flagIndex = tokens.findIndex((token) => token.startsWith("--"));
      const command = tokens.slice(0, flagIndex < 0 ? undefined : flagIndex).join(" ");
      const signature = usage.find((line) => {
        const prefix = line.trim().replace(/<[^>]*>/gu, "").replace(/\[[^\]]*\]/gu, "").trim();
        const commandWords = command.split(" ");
        const words = prefix.split(/\s+/u).slice(1);
        return commandWords.every((word, index) => words[index]?.split("|").includes(word));
      });
      assert.ok(signature, `${filename}: undocumented Axis command ${command}`);
      const supported = new Set(signature.match(/--[a-z][a-z0-9-]*/gu));
      for (const flag of invocation.match(/--[a-z][a-z0-9-]*/gu) ?? []) {
        assert.ok(supported.has(flag), `${filename}: ${flag} is not supported by axis ${command}`);
      }
    }
  }
  assert.ok(checked > 0, "no Axis command examples were checked");
});
