# Verification tooling: a zero must prove itself

An empty search result can mean either that content is absent or that the tool
failed to read the intended target. Verification must distinguish those cases.
NUL-containing files can trigger binary-search heuristics; invalid Git refs or
paths can leave a downstream search with no input; commit ancestry cannot prove
content absence after a squash merge.

## The contract

**Any conclusion of the form "not found" / "zero references" / "does not
exist" must carry the anchor hit that proves the tool which produced that
zero actually works against that exact target.** A zero with no such anchor
is not evidence of absence -- write it up as "unverified", not as "not
found", and say so explicitly rather than letting the zero stand in for a
conclusion it cannot support. This applies to prose written into a ticket,
a PR description, a merged document, or a code comment, not only to
automated gates.

## The tooling

Three scripts, each importable (for testing and reuse) and independently
runnable as a CLI. None of the three is on the production runtime surface
-- they are development/verification tooling only, listed in
`scripts/package-surface-guardrails.mjs`'s `activeDistBases` only insofar
as `check:scripts` type-checks them, never exported from `src/index.ts`.

### `scripts/checked-search.mjs`

A checked text search over a local file or a `<ref>:<path>` git target.

- Reads the target's raw bytes and decodes them as UTF-8 directly -- it
  never shells out to `grep`, so it is not subject to any grep
  implementation's own binary-detection heuristic.
- A git ref that does not resolve, or a path that does not exist at that
  ref, is an **error**, never a silent empty read:
  `git rev-parse --verify` runs before `git show`.
- **A zero-hit pattern is only reported once a caller-supplied `anchor`
  string -- one already known to exist in that exact target -- also
  matches at least once in the same read.** If the anchor itself matches
  zero times, the tool throws rather than reporting the pattern's zero:
  the reader cannot be trusted for this target, so neither can its zero.

```
node scripts/checked-search.mjs --pattern 'run.waiting_for_input' --anchor 'import' --file src/worker/command.ts
node scripts/checked-search.mjs --pattern 'someSymbol' --anchor 'import' --ref origin/some-branch --path src/some/file.ts
```

### `scripts/merged-by-symbol.mjs`

Decides "did this content actually reach the target tree" by comparing a
symbol's occurrence in `sourceRef`'s tree against `targetRef`'s tree, using
`git grep` against both trees rather than `--is-ancestor`. This checks symbol
presence, not patch equivalence; binary-file handling still belongs to Git.

```
node scripts/merged-by-symbol.mjs <symbol> <sourceRef> <targetRef>
```

Throws if `symbol` does not even appear at `sourceRef` (comparing a typo'd
or never-introduced symbol against the target would produce a meaningless
"absent" that looks identical to a real one), and if either ref does not
resolve. Reports a `verdict` (`present_in_target` / `absent_from_target`)
plus the occurrence counts and file lists on both sides -- never a bare
boolean, since a distinctive symbol's presence in the target is strong
evidence of having merged, not a certainty this tool can rule every
alternative explanation out for.

### `scripts/check-environment.mjs`

Asserts, before any gate that depends on `tsc`, that the current
`node_modules` tree actually has `tsc` and resolves
`@axisosme/axis-contracts` to exactly the version the current checkout requires. An
emptied tree produces `sh: tsc: command not found`; a tree pinned to the
wrong contracts version produces `tsc` type errors across files a branch
never touched -- both read exactly like a real regression until this
check names the actual tool problem instead. Wired as `npm run
check:environment`, the first step of `npm run check`, so a broken
dependency tree is reported by name before it can be misread as a code
regression by anything downstream of it.

```
npm run check:environment
```

## Limits

These tools validate specific search and environment assumptions. They do not
make every command self-verifying. Direct searches, Git pipelines, and ancestry
checks still need their target and reader validated before an absence claim.
