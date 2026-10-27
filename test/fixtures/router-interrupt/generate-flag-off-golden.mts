// Records the current Linear scenario's flag-off baseline with fixed Router clock and ids.
// Run from the repository root with Node 24:
//   node --import=tsx test/fixtures/router-interrupt/generate-flag-off-golden.mts
import { writeFileSync } from "node:fs";
import { runRouterInterruptScenario } from "../../support/router-interrupt-scenario.ts";

const record = await runRouterInterruptScenario({ nativeSteer: false });
writeFileSync(new URL("./flag-off-golden.json", import.meta.url), `${JSON.stringify({
  provenance: {
    description: "Current Linear flag-off baseline for a follow-up on a running Run.",
    scenario: "test/support/router-interrupt-scenario.ts",
    nativeSteer: false,
    writer: "Router.startNode and Router.continueNode through NodeDispatchDouble",
    generator: "test/fixtures/router-interrupt/generate-flag-off-golden.mts",
  },
  record,
}, null, 2)}\n`);
