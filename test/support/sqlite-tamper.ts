import { DatabaseSync } from "node:sqlite";
import { statSync } from "node:fs";

/**
 * SQLite damage and inspection helpers for readiness and hot-path cost tests. Each
 * helper preserves the distinction between data corruption and schema changes so
 * callers can exercise the intended integrity boundary.
 */

/**
 * One edit applied through a SEPARATE connection with foreign keys off -- the
 * way external damage, a partially-restored backup, or an operator's manual
 * repair actually arrives.
 *
 * A second connection rather than the store's own handle on purpose: damage a
 * store could have written itself is not the damage these suites are about.
 */
export function tamper(filename: string, sql: string): void {
  const database = new DatabaseSync(filename);
  try {
    database.exec("PRAGMA foreign_keys=OFF;");
    database.exec(sql);
  } finally {
    database.close();
  }
}

/**
 * Every schema object in one file.
 *
 * Callers snapshot this before and after a tamper and assert equality, which
 * is what proves the damage is invisible to a schema sweep. Without that
 * assertion a "the cheap check cannot see this" case would also pass against
 * damage the cheap check catches easily.
 */
export function schemaObjects(filename: string): readonly unknown[] {
  const database = new DatabaseSync(filename, { readOnly: true });
  try {
    return database.prepare("SELECT type, name, sql FROM sqlite_master ORDER BY name").all();
  } finally {
    database.close();
  }
}

/**
 * Measure database bytes including WAL sidecars. Fresh writes can remain entirely in
 * the WAL until checkpoint, so main-file size alone cannot establish fixture growth or
 * the data a full integrity scan would read. Accept a path list so callers can pass
 * their differently shaped database maps without casts.
 */
export function totalBytes(filenames: readonly string[]): number {
  return filenames.reduce((sum, filename) => {
    for (const suffix of ["", "-wal", "-shm"]) {
      try { sum += statSync(`${filename}${suffix}`).size; } catch { /* absent until first write */ }
    }
    return sum;
  }, 0);
}
