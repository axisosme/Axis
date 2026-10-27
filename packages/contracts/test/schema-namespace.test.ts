import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import { createRequire } from "node:module";
import test from "node:test";
import type { Ajv2020 as Ajv2020Type } from "ajv/dist/2020.js";
import { fromRuntimePackageRoot } from "../src/runtime-paths.js";

const require = createRequire(import.meta.url);
const Ajv2020 = (require("ajv/dist/2020") as { default: new (options?: Record<string, unknown>) => Ajv2020Type }).default;
const addFormats = (require("ajv-formats") as { default: (ajv: Ajv2020Type) => void }).default;

for (const family of ["runtime", "agent-activity", "linear-execution"]) {
  test(`${family} schemas use the public namespace and resolve all references offline`, async () => {
    const ajv = new Ajv2020({ strict: true, loadSchema: undefined });
    addFormats(ajv);
    const ids: string[] = [];
    const families = family === "agent-activity" ? ["runtime", family] : [family];
    for (const schemaFamily of families) {
      const directory = fromRuntimePackageRoot("schemas", schemaFamily);
      const files = (await readdir(directory)).filter((file) => file.endsWith(".schema.json")).sort();
      assert.ok(files.length > 0);
      for (const file of files) {
        const schema = JSON.parse(await readFile(fromRuntimePackageRoot("schemas", schemaFamily, file), "utf8")) as { $id: string };
        assert.equal(schema.$id, `https://github.com/axisosme/Axis/schemas/${schemaFamily}/${file}`);
        ajv.addSchema(schema);
        ids.push(schema.$id);
      }
    }
    for (const id of ids) assert.ok(ajv.getSchema(id), `${id} must compile without fetching references`);
  });
}
