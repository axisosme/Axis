# Schema registry

Canonical schemas use JSON Schema Draft 2020-12. The package has one current
contract per protocol, each accepting exactly wire version `1.0.0`.

| Protocol | Schema directory and identifier base | Code export |
| --- | --- | --- |
| Runtime | `schemas/runtime` | `./runtime` |
| Agent Activity | `schemas/agent-activity` | `./agent-activity` |
| Linear execution | `schemas/linear-execution` | `./linear-execution` |

Every `$id` is `https://github.com/axisosme/Axis/schemas/<family>/<file>`.
These URLs are identifiers, not remote fetch endpoints. Consumers load the
schemas bundled in the package through neutral `./schemas/*` resource subpaths.
Agent Activity loads its own waiting and response schemas together with the
Runtime envelopes and common definitions referenced by absolute `$id`.

## Validation profile

Validators use Ajv's Draft 2020-12 class in strict mode. Runtime and Agent
Activity assert `date-time`; the credential registry registers the standard
format set. Registries do not coerce values, insert defaults, remove properties,
or fetch remote references. Closed schemas reject unknown canonical fields.
Internal validators share one compiled registry per protocol in each process;
a rejected registry build can be retried. Public registry factories create
independent registries for consumers that need schema inspection.

Schema checks and semantic checks are separate. Semantic validators enforce
identity, causation, lifecycle, continuation, display safety, truncation, and
replay rules and return bounded safe diagnostics. Callers cannot supply schema
paths or reference targets. Generated TypeScript types agree with the canonical
schemas, and generation checks detect drift.

The contracts define only current behavior. There is no alias, downgrade,
version selection, provider discovery, or fallback contract selection.
