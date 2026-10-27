import { DatabaseSync } from "node:sqlite";

/**
 * Capture SQL statements to check that readiness and hot-path operations avoid
 * whole-database integrity_check and foreign_key_check scans. Statement inspection
 * measures the cost-producing operation directly; timing a small database could hide a
 * scan behind machine-speed and scheduling noise.
 */

/**
 * Every SQL statement issued on ANY `node:sqlite` handle while `operation`
 * runs.
 *
 * Patched onto `DatabaseSync.prototype` rather than injected, because that is
 * the only seam there is: each store builds its own handle inside its
 * constructor and keeps it in a private field. That is also why the patch is
 * sound rather than merely convenient -- none of these stores caches a
 * prepared statement, every one of them calls `prepare` freshly on each use,
 * so what this records is the real per-call statement set and not a plan
 * prepared once at construction.
 *
 * `exec` is patched alongside `prepare` because transaction control
 * (`BEGIN DEFERRED`, `COMMIT`) goes through it; a recorder that watched only
 * `prepare` would report a shorter list than the process actually issues.
 *
 * The patch is process-global while it is installed, which is safe for a
 * SYNCHRONOUS callback and would not be everywhere: `node:test` runs cases one
 * at a time, so nothing else in the process is touching a handle. An `async`
 * operation would need its own seam -- see `recordStatementsAsync`, which
 * states what it can and cannot promise.
 */
export function recordStatements<T>(operation: () => T): { readonly value: T; readonly statements: readonly string[] } {
  const statements: string[] = [];
  const restore = install(statements);
  try {
    return { value: operation(), statements };
  } finally {
    restore();
  }
}

/**
 * Record statements across an awaited asynchronous operation. The prototype patch stays
 * installed until the operation finishes, so concurrent DatabaseSync work is also
 * captured. Compare relevant statement shapes and equivalent runs rather than assuming
 * an exact count attributable only to operation.
 */
export async function recordStatementsAsync<T>(operation: () => Promise<T>): Promise<{ readonly value: T; readonly statements: readonly string[] }> {
  const statements: string[] = [];
  const restore = install(statements);
  try {
    return { value: await operation(), statements };
  } finally {
    restore();
  }
}

function install(statements: string[]): () => void {
  const prepare = DatabaseSync.prototype.prepare;
  const exec = DatabaseSync.prototype.exec;
  DatabaseSync.prototype.prepare = function patchedPrepare(this: DatabaseSync, sql: string, ...rest: readonly unknown[]) {
    statements.push(sql);
    return (prepare as (this: DatabaseSync, ...args: readonly unknown[]) => unknown).call(this, sql, ...rest);
  } as typeof DatabaseSync.prototype.prepare;
  DatabaseSync.prototype.exec = function patchedExec(this: DatabaseSync, sql: string, ...rest: readonly unknown[]) {
    statements.push(sql);
    return (exec as (this: DatabaseSync, ...args: readonly unknown[]) => unknown).call(this, sql, ...rest);
  } as typeof DatabaseSync.prototype.exec;
  return () => {
    DatabaseSync.prototype.prepare = prepare;
    DatabaseSync.prototype.exec = exec;
  };
}

/** Whole-database scans that readiness and per-method integrity guards must not issue. */
export const SCAN_PRAGMA = /integrity_check|foreign_key_check/iu;

/**
 * Whether one statement's cost is bounded by the SCHEMA rather than by the
 * rows the schema describes.
 *
 * A predicate, deliberately not a hardcoded list of the exact SQL a path is
 * expected to issue. A copied-out list is a second transcription of the
 * implementation: it passes whenever the two copies agree, including when the
 * implementation has been changed to something equally expensive but
 * differently spelled, and it has to be hand-updated for every innocent
 * rewording. This asks the property the readiness checks require -- does this
 * statement's runtime depend on how much data the store holds -- so a future
 * statement that violates it fails whether or not anyone remembers this file
 * exists.
 *
 * The three admitted shapes:
 *
 * - transaction control, which touches no rows;
 * - a read of `sqlite_master`, bounded by the number of schema objects;
 * - a read of a `*_schema` ledger, which records one current schema row.
 */
export function schemaBounded(sql: string): boolean {
  const normalized = sql.replace(/\s+/gu, " ").trim().toUpperCase().replace(/;$/u, "");
  if (["BEGIN", "BEGIN DEFERRED", "BEGIN IMMEDIATE", "COMMIT", "ROLLBACK"].includes(normalized)) return true;
  if (!normalized.startsWith("SELECT ")) return false;

  // Every source the statement reads must be schema-bounded, not just one of
  // them. Matching a single `FROM sqlite_master` would admit
  // `SELECT ... FROM sqlite_master JOIN channel_run_chain_runs ...`, which
  // reads the schema AND every Run.
  // `?? ""` rather than a non-null assertion: an unnamed source is a source
  // this predicate cannot vouch for, and the empty string satisfies neither
  // arm of the `every` below, so it fails closed.
  const sources = [...normalized.matchAll(/\b(?:FROM|JOIN)\s+([A-Z0-9_]+)/gu)].map((match) => match[1] ?? "");
  // A source this regex could not name -- a subquery, a table-valued
  // function, a quoted identifier -- is not a source this predicate can
  // vouch for, so it fails rather than being skipped. Zero of both is a
  // constant `SELECT` with no source at all, which is bounded by nothing.
  if (sources.length !== (normalized.match(/\b(?:FROM|JOIN)\b/gu) ?? []).length) return false;
  return sources.every((name) => name === "SQLITE_MASTER" || name.endsWith("_SCHEMA"));
}
