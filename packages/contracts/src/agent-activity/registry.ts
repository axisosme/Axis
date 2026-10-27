import { readFile, readdir } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import type { Ajv2020 as Ajv2020Type, ValidateFunction } from "ajv/dist/2020.js";
import { fromRuntimePackageRoot } from "../runtime-paths.js";

export const agentActivitySchemaVersion = "1.0.0" as const;
export const agentActivitySchemaBase = `https://github.com/axisosme/Axis/schemas/agent-activity`;
export const agentActivitySchemaNames = ["agent-activity-elicitation-receipt", "executor-agent-waiting-delivery", "run-agent-waiting-delivery", "task-agent-waiting-delivery", "agent-activity-response-receipt", "executor-agent-response-delivery", "run-agent-response-delivery", "task-agent-response-delivery"] as const;
export type AgentActivitySchemaName = (typeof agentActivitySchemaNames)[number];
export const agentActivitySchemaIds = Object.fromEntries(agentActivitySchemaNames.map((name) => [name, `${agentActivitySchemaBase}/${name}.schema.json`])) as Record<AgentActivitySchemaName, string>;

const require = createRequire(import.meta.url);
const Ajv2020 = (require("ajv/dist/2020") as { default: new (options?: Record<string, unknown>) => Ajv2020Type }).default;
const addFormats = (require("ajv-formats") as { default: (ajv: Ajv2020Type, formats?: string[]) => Ajv2020Type }).default;
export interface AgentActivitySchemaRegistry { ajv: Ajv2020Type; schemaIds: readonly string[]; }

async function schemasAt(family: string): Promise<Record<string, unknown>[]> {
  const directory = fromRuntimePackageRoot("schemas", family);
  const files = (await readdir(directory)).filter((file) => file.endsWith(".json")).sort();
  return Promise.all(files.map(async (file) => JSON.parse(await readFile(path.join(directory, file), "utf8")) as Record<string, unknown>));
}

// Agent Activity references its Runtime envelope/common schemas by absolute $id.
// Load the matching schema directory once so references share schema definitions.
export async function createAgentActivitySchemaRegistry(): Promise<AgentActivitySchemaRegistry> {
  const ajv = new Ajv2020({ allErrors: true, strict: true, validateFormats: true, coerceTypes: false, removeAdditional: false, useDefaults: false, loadSchema: undefined });
  addFormats(ajv, ["date-time"]);
  const schemas = [...await schemasAt("runtime"), ...await schemasAt("agent-activity")];
  for (const schema of schemas) ajv.addSchema(schema);
  return { ajv, schemaIds: schemas.map((schema) => String(schema.$id)).sort() };
}

export function agentActivityValidatorFor(registry: AgentActivitySchemaRegistry, name: AgentActivitySchemaName): ValidateFunction {
  const validator = registry.ajv.getSchema(agentActivitySchemaIds[name]);
  if (!validator) throw new Error(`Agent activity schema is not registered for ${name}`);
  return validator;
}
