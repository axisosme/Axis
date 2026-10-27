import assert from "node:assert/strict";
import { createRequire } from "node:module";
import test from "node:test";
import { validateAgentActivityElicitationReceipt as validateAgentActivityElicitationReceipt } from "../src/agent-activity/validate.js";
import { validateAgentActivityResponseReceipt } from "../src/agent-activity/validate.js";
import { memoizedRegistry } from "../src/registry-cache.js";
import { createRuntimeSchemaRegistry } from "../src/runtime/registry.js";
import { validateTask as validateRuntimeTask } from "../src/runtime/validate.js";

// Count every schema added to any Ajv instance. The registries resolve `ajv/dist/2020` through the same
// CommonJS module cache as this test, so patching the prototype that owns `addSchema` observes all of them.
const require = createRequire(import.meta.url);
const Ajv2020 = (require("ajv/dist/2020") as { default: { prototype: object } }).default;
let owner: object | null = Ajv2020.prototype;
while (owner !== null && !Object.hasOwn(owner, "addSchema")) owner = Object.getPrototypeOf(owner);
assert.ok(owner !== null, "Ajv must define addSchema on its prototype chain");
const ajvPrototype = owner as { addSchema: (...args: unknown[]) => unknown };
const addSchema = ajvPrototype.addSchema;
let schemasAdded = 0;
ajvPrototype.addSchema = function countedAddSchema(this: unknown, ...args: unknown[]) {
  schemasAdded += 1;
  return addSchema.apply(this, args);
};

test("a memoized registry is built once for sequential and concurrent callers", async () => {
  let builds = 0;
  const shared = memoizedRegistry(async () => ({ build: ++builds }));
  const concurrent = await Promise.all([shared(), shared(), shared()]);
  const sequential = [await shared(), await shared()];
  assert.equal(builds, 1);
  for (const registry of [...concurrent, ...sequential]) assert.equal(registry, concurrent[0]);
});

test("a rejected registry build is not kept, so the next caller builds again", async () => {
  let builds = 0;
  const shared = memoizedRegistry(async () => {
    builds += 1;
    if (builds === 1) throw new Error("transient readdir failure");
    return { build: builds };
  });
  await assert.rejects(shared(), /transient readdir failure/);
  // Retrying immediately after observing the rejection must start a fresh build, never replay the failure.
  assert.deepEqual(await shared(), { build: 2 });
  assert.deepEqual(await shared(), { build: 2 });
  assert.equal(builds, 2);
});

test("the public registry factory still returns a new registry on every call", async () => {
  const first = await createRuntimeSchemaRegistry();
  const second = await createRuntimeSchemaRegistry();
  assert.notEqual(first, second);
  assert.notEqual(first.ajv, second.ajv);
});

// Group internal validator call sites by their shared registry family. Invalid inputs still exercise
// schema validation while excluding later semantic work.
const sites: readonly { readonly group: string; readonly site: string; readonly validate: () => Promise<unknown> }[] = [
  { group: "runtime", site: "validateRuntimeSchema", validate: () => validateRuntimeTask({}) },
  { group: "agent-activity", site: "validateAgentActivityElicitationReceipt", validate: () => validateAgentActivityElicitationReceipt({}) },
  { group: "agent-activity", site: "validateAgentActivityResponseReceipt", validate: () => validateAgentActivityResponseReceipt({}) },
];

test("repeated validation reuses one compiled registry per contract family and never rebuilds it", async () => {
  const addedByFirstValidationOfGroup = new Map<string, number>();
  for (const { group, site, validate } of sites) {
    const beforeFirst = schemasAdded;
    await validate();
    if (!addedByFirstValidationOfGroup.has(group)) addedByFirstValidationOfGroup.set(group, schemasAdded - beforeFirst);
    const afterFirst = schemasAdded;
    for (let repeat = 0; repeat < 5; repeat += 1) await validate();
    assert.equal(schemasAdded - afterFirst, 0, `${group} ${site}: repeating a validation rebuilt the schema registry`);
  }
  for (const [group, added] of addedByFirstValidationOfGroup) {
    assert.ok(added > 0, `${group}: its first validation must build the registry, or this test observes nothing for it`);
  }
  assert.equal(addedByFirstValidationOfGroup.size, 2);
});
