import { DatabaseSync } from "node:sqlite";
import { types } from "node:util";

/**
 * Input validation, value hygiene and the SQLite connection the durable stores share.
 *
 * The pure validators and copy helpers are handed out per store by `durableStoreInput`, so each
 * rejects with that store's own input error. `DurableStoreConnection` owns a store's handle, its
 * open/closed state and its snapshot depth.
 *
 * Still per store: `#transaction` (the channel stores short-circuit a re-entrant call; the others
 * let SQLite reject the nested `BEGIN IMMEDIATE`), `#assertInvariants`, and `options()`.
 *
 * Each store declares its own `malformed()`: TypeScript narrows after a never-returning call only
 * when the callee's declaration carries an explicit type, which a destructured function would not.
 */

/** The identifier policy every store applies to opaque caller-supplied strings. */
export const DURABLE_STORE_IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

/** A plain, non-array object. Arrays are excluded so `canonical` can treat them separately. */
export const isObject = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);

/**
 * A stable string for a JSON-shaped value: object keys sorted, arrays in order. Used for digests
 * and equality over persisted JSON.
 */
export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (isObject(value)) return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(",")}}`;
  return JSON.stringify(value);
}

/** Deep-freeze in place, returning the same reference. */
export function frozen<T>(value: T): T {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value as Record<string, unknown>)) frozen(child);
    Object.freeze(value);
  }
  return value;
}

/** A deep, frozen, structurally-plain copy -- what these stores hand back to callers. */
export function copy<T>(value: T): T { return frozen(JSON.parse(JSON.stringify(value)) as T); }

/** The caller-bound validators. Each rejects by throwing the store's own input error. */
export interface DurableStoreInputValidators {
  /**
   * The value as a plain data record, or a rejection.
   *
   * Rejects a proxy, a non-`Object.prototype` prototype, symbol keys, and any
   * accessor or non-enumerable property -- so what comes back cannot re-enter
   * caller code on read. With `allowed`, the key set must match it exactly.
   */
  readonly plainData: (value: unknown, allowed?: readonly string[]) => Record<string, unknown>;
  /** A caller-supplied identifier string, or a rejection. */
  readonly opaque: (value: unknown) => string;
  /** A non-negative safe integer revision, or a rejection. */
  readonly expected: (value: unknown) => number;
}

/**
 * Bind the shared validators to one store's input-error type. A constructor rather than a thrower,
 * because `plainData` also recognises its own rejection to rethrow it unwrapped.
 */
export function durableStoreInput(InputError: new () => Error): DurableStoreInputValidators {
  // Explicitly typed so TypeScript keeps narrowing after a call to it.
  const malformed: () => never = () => { throw new InputError(); };

  const plainData = (value: unknown, allowed?: readonly string[]): Record<string, unknown> => {
    try {
      if (!isObject(value) || types.isProxy(value) || Object.getPrototypeOf(value) !== Object.prototype || Object.getOwnPropertySymbols(value).length !== 0) malformed();
      const descriptors = Object.getOwnPropertyDescriptors(value), actual = Object.keys(descriptors).sort();
      if (Object.values(descriptors).some((descriptor) => !descriptor.enumerable || descriptor.get !== undefined || descriptor.set !== undefined) || (allowed !== undefined && (actual.length !== allowed.length || actual.some((key, index) => key !== [...allowed].sort()[index])))) malformed();
      return Object.fromEntries(actual.map((key) => [key, descriptors[key]?.value]));
    } catch (error) { if (error instanceof InputError) throw error; malformed(); }
  };

  const opaque = (value: unknown): string => { if (typeof value !== "string" || !DURABLE_STORE_IDENTIFIER.test(value)) malformed(); return value; };
  const expected = (value: unknown): number => { if (!Number.isSafeInteger(value) || (value as number) < 0) malformed(); return value as number; };

  return { plainData, opaque, expected };
}

/**
 * The error vocabulary a connection speaks, supplied by the store that owns it. It serves the
 * `DatabaseSync` construction catch, `initialize`'s close-on-failure, and `readSnapshot`'s rollback.
 */
export interface DurableStoreConflictPolicy {
  /** The error every connection-level failure becomes. */
  readonly conflict: () => Error;
  /**
   * Errors rethrown unchanged instead of being replaced by `conflict()`. Every store preserves its
   * own store error; the channel stores also preserve `ChannelSessionRefusalError`, which
   * `SqliteChannelRunChainStore#continuationContext` throws from inside a snapshot as a public
   * refusal.
   */
  readonly preserve: (error: unknown) => boolean;
}

/**
 * One store's SQLite handle, its open/closed state, and its snapshot depth. The store keeps its own
 * `#db` pointing at the same handle. `inTransaction` is writable because the store's
 * `#transaction` toggles the same flag `readSnapshot` reads.
 */
export class DurableStoreConnection {
  readonly #db: DatabaseSync;
  readonly #policy: DurableStoreConflictPolicy;
  #closed = false;
  #inTransaction = false;

  constructor(db: DatabaseSync, policy: DurableStoreConflictPolicy) {
    this.#db = db;
    this.#policy = policy;
  }

  get db(): DatabaseSync { return this.#db; }
  get inTransaction(): boolean { return this.#inTransaction; }
  set inTransaction(value: boolean) { this.#inTransaction = value; }

  assertOpen(): void { if (this.#closed) throw this.#policy.conflict(); }

  close(): void { if (!this.#closed) { this.#db.close(); this.#closed = true; } }

  /**
   * Run a construction step; if it throws, close the handle and convert the error through the
   * policy. Used for the PRAGMA sequence and each store's `#initializeSchema()` and `#assertInvariants()`. Not
   * for use after construction: it closes the handle on failure.
   */
  initialize(operation: () => void): void {
    try { operation(); }
    catch (error) {
      try { this.#db.close(); this.#closed = true; } catch { /* preserve the safe construction failure */ }
      if (this.#policy.preserve(error)) throw error;
      throw this.#policy.conflict();
    }
  }

  /**
   * Multi-statement reads share one snapshot, since under WAL a reader can see a new commit between
   * two statements. A caller already inside `#transaction` reuses it without nesting.
   */
  readSnapshot<T>(operation: () => T): T {
    if (this.#inTransaction) return operation();
    this.assertOpen();
    try { this.#db.exec("BEGIN DEFERRED"); }
    catch { throw this.#policy.conflict(); }
    this.#inTransaction = true;
    try {
      const result = operation();
      try { this.#db.exec("COMMIT"); } catch { try { this.#db.exec("ROLLBACK"); } catch { /* best effort */ } throw this.#policy.conflict(); }
      return result;
    } catch (error) {
      try { this.#db.exec("ROLLBACK"); } catch { /* best effort */ }
      if (this.#policy.preserve(error)) throw error;
      throw this.#policy.conflict();
    } finally { this.#inTransaction = false; }
  }
}

/**
 * Open a store's database: `journal_mode=WAL` for concurrent readers, `foreign_keys=ON` because
 * every schema declares them, `synchronous=FULL` so the last write survives a crash. A failure
 * becomes the store's conflict error with the handle already closed.
 */
export function openDurableStoreConnection(
  options: { readonly filename?: string; readonly busyTimeoutMs?: number },
  policy: DurableStoreConflictPolicy,
): DurableStoreConnection {
  let db: DatabaseSync;
  try { db = new DatabaseSync(options.filename ?? ":memory:"); }
  catch (error) { if (policy.preserve(error)) throw error; throw policy.conflict(); }

  const connection = new DurableStoreConnection(db, policy);
  connection.initialize(() => {
    db.exec("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;");
    db.exec(`PRAGMA busy_timeout=${options.busyTimeoutMs ?? 5_000};`);
  });
  return connection;
}
