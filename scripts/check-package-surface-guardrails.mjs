import { assertCurrentBuildArtifactInventory, assertDeclaredSurface } from "./package-surface-guardrails.mjs";

assertDeclaredSurface();
if (process.argv[2] !== "--source-only") assertCurrentBuildArtifactInventory();
process.stdout.write("Active root export and emitted-artifact drift checks passed.\n");
