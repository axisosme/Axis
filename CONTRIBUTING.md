# Contributing to Axis

Use Node 24. Install dependencies and run the full local validation gate:

```sh
npm install
npm run check
```

The full gate covers static checks, Relay and Node tests, and package/install
checks. Run the narrower suites while working on a focused change:

```sh
npm run check:static
npm run test:relay
npm run test:node
npm run check:distribution
```

`npm run test:node` includes the Node end-to-end suite. For contracts changes,
also run the contracts workspace gate:

```sh
npm run ci --workspace @axisosme/axis-contracts
```

Deployment documentation is organized into [single-host setup](docs/SINGLE-HOST-DEPLOYMENT.md),
[multi-host operations](docs/MULTI-HOST-DEPLOYMENT.md) and the
[configuration reference](docs/CONFIGURATION.md). Check relative links and
anchors when moving sections, and keep command examples aligned with the CLI.

## Pull requests

- Keep changes small and focused. Explain the problem, resulting behavior, and
  validation results in the pull request.
- Add or update tests when behavior changes. Run the relevant suites, inspect
  their results, and run `npm run check` before requesting review. Disclose any
  checks you could not run.
- Keep `openapi.yaml`, API documentation, and contract schemas consistent with
  applicable changes. Regenerate contract types with the workspace generators
  when their source schemas change.
- Write comments about the current code. Follow [AGENTS.md](AGENTS.md) for code
  style, component responsibilities, and test/package registration rules.
- Keep credentials, personal data, prompts, and provider output out of changes
  and diagnostics. With Gitleaks installed, run
  `gitleaks dir . --no-banner --redact` before submitting.

Use the issue forms for bug reports and feature requests. Report vulnerabilities
through the private channel in [SECURITY.md](SECURITY.md). Participation follows
the [Code of Conduct](CODE_OF_CONDUCT.md).
