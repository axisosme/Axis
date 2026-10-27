import { createCipheriv, createDecipheriv, createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { access, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

import { RelayError } from "./errors.js";
import type { CredentialVault, LinearHeaders } from "./types.js";

const SIGNATURE_PATTERN = /^[a-f0-9]{64}$/i;
const DELIVERY_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function sha256(value: Uint8Array | string): string {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

export function verifyLinearWebhook(
  rawBody: Buffer,
  headers: LinearHeaders,
  signingSecret: string,
  nowMs: number,
  toleranceMs = 60_000,
): void {
  if (!DELIVERY_PATTERN.test(headers.deliveryId) || !SIGNATURE_PATTERN.test(headers.signature)) {
    throw new RelayError("linear.ingress.invalid_signature", "The webhook signature is invalid.", 401);
  }
  const supplied = Buffer.from(headers.signature, "hex");
  const expected = createHmac("sha256", signingSecret).update(rawBody).digest();
  if (supplied.length !== expected.length || !timingSafeEqual(supplied, expected)) {
    throw new RelayError("linear.ingress.invalid_signature", "The webhook signature is invalid.", 401);
  }
  const timestamp = Number(headers.timestamp);
  if (!Number.isFinite(timestamp) || Math.abs(nowMs - timestamp) > toleranceMs) {
    throw new RelayError("linear.ingress.stale_timestamp", "The webhook timestamp is outside the accepted window.", 401);
  }
}

export class EnvelopeCipher {
  readonly #key: Buffer;

  constructor(base64Key: string) {
    const key = Buffer.from(base64Key, "base64");
    if (key.length !== 32) {
      throw new Error("Relay master key must decode to exactly 32 bytes.");
    }
    this.#key = key;
  }

  encrypt(plaintext: string): string {
    const nonce = randomBytes(12);
    const cipher = createCipheriv("aes-256-gcm", this.#key, nonce);
    const ciphertext = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
    return ["v1", nonce.toString("base64url"), cipher.getAuthTag().toString("base64url"), ciphertext.toString("base64url")].join(".");
  }

  decrypt(envelope: string): string {
    const parts = envelope.split(".");
    if (parts.length !== 4 || parts[0] !== "v1") throw new Error("Unsupported encrypted envelope.");
    const nonce = Buffer.from(parts[1] as string, "base64url");
    const tag = Buffer.from(parts[2] as string, "base64url");
    const ciphertext = Buffer.from(parts[3] as string, "base64url");
    const decipher = createDecipheriv("aes-256-gcm", this.#key, nonce);
    decipher.setAuthTag(tag);
    return Buffer.concat([decipher.update(ciphertext), decipher.final()]).toString("utf8");
  }
}

export class EncryptedFileCredentialVault implements CredentialVault {
  readonly ready = true;
  readonly #filename: string;
  readonly #markerFilename: string;
  readonly #cipher: EnvelopeCipher;
  #writeChain: Promise<void> = Promise.resolve();

  constructor(filename: string, cipher: EnvelopeCipher) {
    this.#filename = filename;
    this.#markerFilename = `${filename}.provisioned`;
    this.#cipher = cipher;
  }

  async put(reference: string, value: string): Promise<void> {
    this.#writeChain = this.#writeChain.then(async () => {
      const values = await this.#readAll();
      values[reference] = this.#cipher.encrypt(value);
      await this.#writeAll(values);
    });
    await this.#writeChain;
  }

  async get(reference: string): Promise<string | null> {
    await this.#writeChain;
    const encrypted = (await this.#readAll())[reference];
    return encrypted === undefined ? null : this.#cipher.decrypt(encrypted);
  }

  async delete(reference: string): Promise<void> {
    this.#writeChain = this.#writeChain.then(async () => {
      const values = await this.#readAll();
      delete values[reference];
      await this.#writeAll(values);
    });
    await this.#writeChain;
  }

  async #readAll(): Promise<Record<string, string>> {
    // The marker is checked before the vault is read; do not reorder. The
    // marker is written only after a successful rename, so marker-present
    // followed by ENOENT is a real loss. Checking it after the read could
    // observe a concurrent writer's fresh marker and refuse a vault that exists.
    const marker = await this.#checkMarker();
    try {
      const value: unknown = JSON.parse(await readFile(this.#filename, "utf8"));
      if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("Invalid vault file.");
      // Any vault that reads back is marked, including one restored from
      // elsewhere. The write is idempotent, so it is also attempted when the
      // marker state is indeterminate.
      if (marker.state !== "present") await this.#markProvisioned();
      return value as Record<string, string>;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        // A present marker means the vault was provisioned and is now lost:
        // refuse rather than read it as empty.
        if (marker.state === "present") {
          throw new RelayError("axis.relay.vault_unavailable", "The credential vault is missing and was previously provisioned.", 503);
        }
        if (marker.state === "absent") return {};
        // Indeterminate marker: neither "lost" nor "fresh" can be claimed, so
        // the marker-check failure itself is surfaced.
        throw marker.error;
      }
      throw error;
    }
  }

  async #checkMarker(): Promise<{ state: "present" } | { state: "absent" } | { state: "indeterminate"; error: unknown }> {
    // A non-ENOENT `access()` failure proves nothing about provisioning, so it
    // is "indeterminate" for the caller to interpret. Transient errors
    // (`EMFILE`, `EIO`) get a short bounded retry first.
    const attempts = 3;
    let lastError: unknown;
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      try {
        await access(this.#markerFilename);
        return { state: "present" };
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") return { state: "absent" };
        lastError = error;
        if (attempt < attempts) await new Promise((resolve) => setTimeout(resolve, attempt * 5));
      }
    }
    return { state: "indeterminate", error: lastError };
  }

  async #markProvisioned(): Promise<void> {
    try {
      await writeFile(this.#markerFilename, "1", { encoding: "utf8", mode: 0o600 });
    } catch {
      // Best-effort: the marker carries no data, and failing to write it must
      // not fail an operation whose data already committed or was read.
    }
  }

  async #writeAll(values: Record<string, string>): Promise<void> {
    await mkdir(dirname(this.#filename), { recursive: true, mode: 0o700 });
    const temporary = `${this.#filename}.${process.pid}.${randomBytes(6).toString("hex")}.tmp`;
    // A unique temp name per writer; `wx` also refuses a suffix collision.
    await writeFile(temporary, JSON.stringify(values), { encoding: "utf8", mode: 0o600, flag: "wx" });
    await rename(temporary, this.#filename);
    // The marker follows the rename, so a crash can never leave a marker
    // without a vault.
    await this.#markProvisioned();
  }
}

export function redactSecrets(value: string): string {
  return value
    .replace(/Bearer\s+[A-Za-z0-9._~+\/-]+/gi, "Bearer [REDACTED]")
    .replace(/(access_token|refresh_token|client_secret|code_verifier)\s*[=:]\s*[^\s&,}\"]+/gi, "$1=[REDACTED]");
}
