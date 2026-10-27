#!/usr/bin/env node
import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const baseSchemaFiles = [
  "common.schema.json", "task.schema.json", "attempt.schema.json", "run.schema.json",
  "session-binding.schema.json", "runtime-identity-graph.schema.json", "runtime-start-envelope.schema.json",
  "task-submit-envelope.schema.json", "run-start-envelope.schema.json", "executor-start-envelope.schema.json",
  "executor-accepted-envelope.schema.json", "run-accepted-envelope.schema.json", "task-accepted-envelope.schema.json",
  "executor-progressed-envelope.schema.json", "run-progressed-envelope.schema.json", "task-progressed-envelope.schema.json",
  "executor-succeeded-envelope.schema.json", "run-succeeded-envelope.schema.json", "task-succeeded-envelope.schema.json",
  "executor-failed-envelope.schema.json", "run-failed-envelope.schema.json", "task-failed-envelope.schema.json",
  "runtime-boundary-message.schema.json",
];
const targets = [
  {
    outputPath: path.join(root, "src", "runtime", "types.generated.ts"),
    schemaFiles: [
      ...baseSchemaFiles,
      "executor-waiting-for-input-envelope.schema.json",
      "run-waiting-for-input-envelope.schema.json",
      "task-waiting-for-input-envelope.schema.json",
    ],
  },
];

function pascal(value) {
  return value.replace(/(?:^|[-_.\/])([a-zA-Z0-9])/g, (_, character) => character.toUpperCase());
}

function refName(reference) {
  const definition = reference.match(/#\/\$defs\/([^/]+)$/);
  if (definition) return definition[1] === "contractVersion" ? "RuntimeContractVersion" : pascal(definition[1]);
  return pascal(path.basename(reference).replace(/\.schema\.json$/, ""));
}

function typeFor(schema) {
  if (schema.$ref) return refName(schema.$ref);
  if (Object.hasOwn(schema, "const")) return JSON.stringify(schema.const);
  if (schema.enum) return schema.enum.map((value) => JSON.stringify(value)).join(" | ");
  if (schema.anyOf || schema.oneOf) return (schema.anyOf ?? schema.oneOf).map(typeFor).join(" | ");
  if (schema.type === "array") return `Array<${typeFor(schema.items ?? {})}>`;
  if (schema.type === "object" || schema.properties) {
    const required = new Set(schema.required ?? []);
    const properties = Object.entries(schema.properties ?? {})
      .map(([name, value]) => `  ${JSON.stringify(name)}${required.has(name) ? "" : "?"}: ${typeFor(value)};`)
      .join("\n");
    return `{\n${properties}\n}`;
  }
  if (schema.type === "integer" || schema.type === "number") return "number";
  if (schema.type === "boolean") return "boolean";
  if (schema.type === "null") return "null";
  return "string";
}

function declaration(name, schema) {
  if (schema.type === "object" || schema.properties) {
    const required = new Set(schema.required ?? []);
    const properties = Object.entries(schema.properties ?? {})
      .map(([property, value]) => `  ${JSON.stringify(property)}${required.has(property) ? "" : "?"}: ${typeFor(value)};`)
      .join("\n");
    return `export interface ${name} {\n${properties}\n}`;
  }
  return `export type ${name} = ${typeFor(schema)};`;
}

async function generatedSource(schemaFiles) {
  const schemaDirectory = path.join(root, "schemas", "runtime");
  const sourceFiles = await Promise.all(schemaFiles.map(async (file) => ({ file, contents: await readFile(path.join(schemaDirectory, file), "utf8") })));
  const schemas = sourceFiles.map(({ file, contents }) => {
    const schema = JSON.parse(contents);
    const expectedId = `https://github.com/axisosme/Axis/schemas/runtime/${file}`;
    if (schema.$schema !== "https://json-schema.org/draft/2020-12/schema" || schema.$id !== expectedId) throw new Error(`Invalid runtime schema metadata in ${file}`);
    return { file, schema };
  });
  const common = schemas.find(({ file }) => file === "common.schema.json")?.schema;
  if (!common?.$defs?.contractVersion?.const) throw new Error("Runtime contract version is missing");
  const fingerprint = createHash("sha256").update(sourceFiles.map(({ file, contents }) => `${file}\n${contents}\n`).join("")).digest("hex");
  const definitions = schemas.flatMap(({ schema }) => Object.entries(schema.$defs ?? {}))
    .filter(([name]) => name !== "contractVersion")
    .map(([name, schema]) => declaration(pascal(name), schema));
  const documents = schemas.filter(({ file }) => file !== "common.schema.json").map(({ file, schema }) => declaration(pascal(file.replace(/\.schema\.json$/, "")), schema));
  return [
    `// Generated from schemas/runtime/ by scripts/generate-runtime-types.mjs. Do not edit.`, "",
    `export const runtimeContractVersion = ${JSON.stringify(common.$defs.contractVersion.const)} as const;`,
    `export const runtimeSchemaFingerprint = "sha256:${fingerprint}" as const;`,
    "export type RuntimeContractVersion = typeof runtimeContractVersion;", "", ...definitions, "", ...documents, "",
  ].join("\n");
}

const generatedTargets = await Promise.all(targets.map(async (target) => ({ ...target, generated: await generatedSource(target.schemaFiles) })));
if (process.argv.includes("--print")) {
  for (const target of generatedTargets) process.stdout.write(target.generated);
} else if (process.argv.includes("--write")) {
  await Promise.all(generatedTargets.map((target) => writeFile(target.outputPath, target.generated)));
} else {
  for (const target of generatedTargets) {
    let current = "";
    try { current = await readFile(target.outputPath, "utf8"); } catch { /* stale output */ }
    if (current !== target.generated) {
      process.stderr.write(`Runtime type output is stale; run npm run generate:runtime.\n`);
      process.exitCode = 1;
    }
  }
}
