# Axis Contracts

`@axisosme/axis-contracts` version `0.1.0` contains the current Runtime,
Agent Activity, and Linear execution contracts for Axis. Node.js 24 or newer
is required. Every protocol accepts exactly `contractVersion: "1.0.0"`.

Install dependencies from the repository root with `npm install`.
This workspace uses public npm dependencies and needs no registry token.
`npm pack ./packages/contracts` builds the standalone archive. The root Axis
archive bundles its built contracts dependency.

## Public APIs

There are exactly three code exports and no package-root API:

```ts
import {
  validateRuntimeBoundaryMessage,
  validateRuntimeBoundaryTrace,
} from "@axisosme/axis-contracts/runtime";
import {
  validateAgentWaitingDelivery,
  validateAgentResponseDelivery,
  compareAgentWaitingDeliveries,
  compareAgentResponseDeliveries,
} from "@axisosme/axis-contracts/agent-activity";
import {
  validateLinearExecutionCredentialRequest,
  validateLinearExecutionCredentialResponse,
} from "@axisosme/axis-contracts/linear-execution";
```

Each export supplies its current types, constants, and validators. The Runtime
export also supplies `runRuntimeV1Golden`. The Agent Activity export includes
waiting elicitation, terminal response, truncation, and replay validation.
The Linear execution export validates credential issuance and revocation,
including request/response identity and expiry binding.

Canonical resources are reachable through `./schemas/*`, `./fixtures/*`, and
`./conformance/*`. Their directories are `schemas/runtime`,
`schemas/agent-activity`, `schemas/linear-execution`, `fixtures/agent-activity`,
`conformance/runtime`, and `conformance/agent-activity`. Schema `$id`s identify
the protocol by its neutral family name; resource paths contain no version.

## Contract rules

- [Runtime identity, boundaries, lifecycle, and conformance](docs/runtime.md)
- [Agent Activity delivery, structured Other, response, and replay](docs/agent-activity.md)
- [Source identity and continuation admission](docs/runtime-source-and-delivery.md)
- [Offline schema registry and validation profile](docs/schema-registry.md)

Validators are pure contract checks. They perform no provider calls, credential
issuance, persistence, routing, or execution. Callers retain the trusted authority
and atomic store evidence needed for those operations. Credentials, native
identities, private answers, local paths, and raw provider output do not belong
in public projections or diagnostics.

## Verification

```sh
npm run check:generated --workspace @axisosme/axis-contracts
npm run ci --workspace @axisosme/axis-contracts
```

`generate:runtime` and `generate:agent-activity` regenerate the TypeScript types
from the canonical schemas. The CI command checks generation, typechecks, runs
contract tests, and verifies the archive allowlist and installed package.
Installed conformance exercises all three public code exports, Runtime golden
reports, and both Agent Activity delivery capabilities.
