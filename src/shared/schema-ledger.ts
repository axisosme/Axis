/**
 * Schema-ledger and SQL-fingerprint helpers shared by the versioned SQLite stores.
 *
 * Each store fingerprints every object it owns and compares that against `sqlite_master` on open,
 * so an in-place schema edit is caught, and keeps a version ledger saying which schema a file holds.
 * This module holds only the shared parts: the normalizer, fingerprint derivation, object-set
 * comparison and ledger predicates. Each store keeps its own `sqlite_master` sweep, error type and
 * refusal messages.
 *
 * SQLite strips `IF NOT EXISTS` from `sqlite_master.sql`, and every store writes its definitions
 * bare, so the elision in `normalizedSchemaSql` normalizes a definition that spells the clause.
 * `test/relay/store-schema-parity.test.ts` pins the elision list and asserts every
 * `schemaDefinitions` literal under `src/` is free of `IF NOT EXISTS`.
 */

/**
 * The keywords after which `CREATE ... IF NOT EXISTS` is elided. Changing the list shifts the
 * fingerprint of any definition that spells that form.
 */
export const SCHEMA_IF_NOT_EXISTS_ELIDED_AFTER: readonly string[] = Object.freeze(["TABLE", "TRIGGER", "INDEX"]);

/** One `sqlite_master` row, as every store's sweep selects it. Typed `unknown` because a corrupt row must reach the comparison, not crash the read. */
export interface SchemaObjectRow {
  readonly type?: unknown;
  readonly name?: unknown;
  readonly sql?: unknown;
}

/** One schema-ledger row: the version applied and the instant it was applied. */
export interface SchemaLedgerRow {
  readonly version?: unknown;
  readonly applied_at?: unknown;
}

function sqlIdentifierCharacter(value: string | undefined): boolean { return value !== undefined && /[A-Za-z0-9_$]/u.test(value); }

/**
 * Token-normalized SQL, so a fingerprint compares structure rather than stored whitespace.
 * Identifiers keep their case and quoting, and whitespace inside a string literal is preserved.
 * `IF NOT EXISTS` is elided: the clause is idempotency, not structure.
 */
export function normalizedSchemaSql(value: unknown): string {
  if (typeof value !== "string") throw new Error("Schema SQL must be a string.");
  const tokens: string[] = [];
  for (let index = 0; index < value.length;) {
    const character = value[index]!;
    if (/\s/u.test(character)) { index += 1; continue; }
    if (character === "'" || character === '"') {
      const quote = character, start = index;
      let closed = false;
      index += 1;
      while (index < value.length) {
        if (value[index] !== quote) { index += 1; continue; }
        index += 1;
        if (value[index] === quote) { index += 1; continue; }
        closed = true;
        break;
      }
      if (!closed) throw new Error("Unterminated SQL literal.");
      tokens.push(value.slice(start, index));
      continue;
    }
    if (sqlIdentifierCharacter(character)) {
      const start = index;
      index += 1;
      while (sqlIdentifierCharacter(value[index])) index += 1;
      tokens.push(value.slice(start, index));
      continue;
    }
    const operator = value.slice(index, index + 3) === "->>" ? "->>" : ["!=", "<=", ">=", "<>", "==", "||", "<<", ">>", "->"].find((candidate) => value.startsWith(candidate, index));
    if (operator !== undefined) { tokens.push(operator); index += operator.length; continue; }
    tokens.push(character);
    index += 1;
  }
  const canonical: string[] = [];
  for (let index = 0; index < tokens.length; index += 1) {
    if (tokens[index] === "IF" && tokens[index + 1] === "NOT" && tokens[index + 2] === "EXISTS"
      && SCHEMA_IF_NOT_EXISTS_ELIDED_AFTER.includes(canonical.at(-1) ?? "") && canonical.at(-2) === "CREATE") { index += 2; continue; }
    canonical.push(tokens[index]!);
  }
  if (canonical.at(-1) === ";") canonical.pop();
  return JSON.stringify(canonical);
}

/**
 * `normalizedSchemaSql`, but an unnormalizable value becomes a token string no real object can
 * produce instead of throwing, so a corrupt `sqlite_master` row fails the comparison.
 */
export function safeNormalizedSchemaSql(value: unknown): string {
  try { return normalizedSchemaSql(value); } catch { return " unreadable"; }
}

/**
 * The fingerprint table for one store's schema definitions, keyed
 * `"<type>:<name>"` exactly as a `sqlite_master` sweep row is keyed.
 */
export function schemaFingerprintTable<Key extends string>(definitions: Readonly<Record<Key, string>>): Readonly<Record<Key, string>> {
  return Object.freeze(Object.fromEntries(Object.entries(definitions).map(([key, value]) => [key, normalizedSchemaSql(value)]))) as Readonly<Record<Key, string>>;
}

/**
 * Whether a `sqlite_master` sweep is exactly the schema these fingerprints describe: the same
 * number of objects, no duplicate `type:name`, and every one normalizing to its fingerprint.
 * Returns a boolean so each store keeps its own refusal.
 */
export function schemaObjectsMatch(
  objects: readonly SchemaObjectRow[],
  fingerprints: Readonly<Record<string, string>>,
  normalize: (value: unknown) => string = normalizedSchemaSql,
): boolean {
  const expected = Object.keys(fingerprints);
  return objects.length === expected.length
    && new Set(objects.map((object) => `${String(object.type)}:${String(object.name)}`)).size === expected.length
    && objects.every((object) => fingerprints[`${String(object.type)}:${String(object.name)}`] === normalize(object.sql));
}

/** Whether the ledger records exactly the current schema and a canonical timestamp. */
export function schemaLedgerAgrees(rows: readonly SchemaLedgerRow[], currentVersion: number): boolean {
  return schemaLedgerVersion(rows) === currentVersion
    && typeof rows[0]?.applied_at === "string"
    && Number.isFinite(Date.parse(rows[0].applied_at))
    && new Date(rows[0].applied_at).toISOString() === rows[0].applied_at;
}

/** The single recorded schema version, or `null` for a malformed ledger. */
export function schemaLedgerVersion(rows: readonly SchemaLedgerRow[]): number | null {
  if (rows.length !== 1) return null;
  const version = Number(rows[0]?.version);
  return Number.isSafeInteger(version) && version >= 1 ? version : null;
}
