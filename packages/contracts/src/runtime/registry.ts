import { readFile, readdir } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import type { Ajv2020 as Ajv2020Type, ValidateFunction } from "ajv/dist/2020.js";
import { fromRuntimePackageRoot } from "../runtime-paths.js";

export const runtimeSchemaVersion = "1.0.0" as const;
export const runtimeSchemaBase = `https://github.com/axisosme/Axis/schemas/runtime`;
export const runtimeSchemaNames = [
  "common", "task", "attempt", "run", "session-binding", "runtime-identity-graph", "runtime-start-envelope",
  "task-submit-envelope", "run-start-envelope", "executor-start-envelope", "executor-accepted-envelope", "run-accepted-envelope",
  "task-accepted-envelope", "executor-progressed-envelope", "run-progressed-envelope", "task-progressed-envelope",
  "executor-succeeded-envelope", "run-succeeded-envelope", "task-succeeded-envelope", "executor-failed-envelope",
  "run-failed-envelope", "task-failed-envelope", "executor-waiting-for-input-envelope", "run-waiting-for-input-envelope",
  "task-waiting-for-input-envelope", "runtime-boundary-message",
] as const;
export type RuntimeSchemaName = (typeof runtimeSchemaNames)[number];

export const runtimeSchemaIds: Record<RuntimeSchemaName, string> = Object.fromEntries(
  runtimeSchemaNames.map((name) => [name, `${runtimeSchemaBase}/${name}.schema.json`]),
) as Record<RuntimeSchemaName, string>;

const require = createRequire(import.meta.url);
const Ajv2020 = (require("ajv/dist/2020") as { default: new (options?: Record<string, unknown>) => Ajv2020Type }).default;
const addFormats = (require("ajv-formats") as { default: (ajv: Ajv2020Type, formats?: string[]) => Ajv2020Type }).default;

export interface RuntimeSchemaRegistry {
  ajv: Ajv2020Type;
  schemaIds: readonly string[];
}

export async function createRuntimeSchemaRegistry(): Promise<RuntimeSchemaRegistry> {
  const ajv = new Ajv2020({
    allErrors: true,
    strict: true,
    validateFormats: true,
    coerceTypes: false,
    removeAdditional: false,
    useDefaults: false,
    loadSchema: undefined,
  });
  addFormats(ajv, ["date-time"]);
  const directory = fromRuntimePackageRoot("schemas", "runtime");
  const files = (await readdir(directory)).filter((file) => file.endsWith(".json")).sort();
  const schemas = await Promise.all(files.map(async (file) =>
    JSON.parse(await readFile(path.join(directory, file), "utf8")) as Record<string, unknown>,
  ));
  for (const schema of schemas) ajv.addSchema(schema);
  return { ajv, schemaIds: schemas.map((schema) => String(schema.$id)).sort() };
}

export function runtimeValidatorFor(registry: RuntimeSchemaRegistry, name: RuntimeSchemaName): ValidateFunction {
  const validator = registry.ajv.getSchema(runtimeSchemaIds[name]);
  if (!validator) throw new Error(`Runtime schema is not registered for ${name}`);
  return validator;
}
