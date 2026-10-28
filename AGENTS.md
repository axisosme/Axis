# Contributor guidance

Axis dispatches tasks to AI executors on worker nodes and tracks their execution
lifecycle. Relay, Router, Node, Worker, provider, and executor components own
distinct boundaries; preserve those responsibilities when making changes.

For deployment and upgrade requests, use the [Axis deployment skill](.agents/skills/axis-deploy/SKILL.md).

## Code style

Use TypeScript ESM / NodeNext conventions and the strict settings in
[`tsconfig.json`](tsconfig.json). Match surrounding imports and module style.
Keep only the current code path, without compatibility branches or shims.
Comments describe what the code is now, without ticket references or history.

Characterization and conformance tests provide evidence of existing behavior;
verify behavior against the source and scoped requirements before changing tests.
Keep shared wire schemas in the contracts package, and keep API documentation
and `openapi.yaml` consistent with applicable changes. Keep credentials, prompts,
provider output, native identities, and local paths out of public projections
and diagnostics.

## Reference documentation

- [README](README.md): architecture, setup, and supported commands.
- [API](docs/API.md) and [OpenAPI](openapi.yaml): HTTP and wire behavior.
- [Persistence](docs/PERSISTENCE.md): journals, recovery, and replay.
- [Security](docs/SECURITY-MODEL.md): authentication and sensitive data handling.
- [Executor adapters](docs/EXECUTOR-ADAPTER.md): native execution and lifecycle.
- [Linear provider](docs/LINEAR-PROVIDER.md): Linear ingress and output.
- [Single-host deployment](docs/SINGLE-HOST-DEPLOYMENT.md): setup, services and maintenance.
- [Multi-host deployment](docs/MULTI-HOST-DEPLOYMENT.md): routing and managed host lifecycle.
- [Configuration](docs/CONFIGURATION.md): TOML schema and private key ownership.

## Local validation

Use Node 24. Install dependencies with `npm install`, then run:

```sh
npm run build
npm run typecheck
npm run test:relay
npm run test:node
npm run check
```

Run relevant tests and inspect their results; a build alone does not verify
behavior. `npm run check` is the full integration gate. Register new test files
in the matching package test script and reconcile source artifacts and exports
with `scripts/package-surface-guardrails.mjs`. For documentation changes, check
relative links, rule meaning, scope, and `git diff --check`.
