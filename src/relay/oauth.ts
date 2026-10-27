import { createHash, randomBytes } from "node:crypto";

import { RelayError } from "./errors.js";
import type { LinearProviderPort } from "./linear/provider.js";
import { EnvelopeCipher, sha256 } from "./security.js";
import type { LinearInstallationOAuthPort } from "./linear/installation.js";
import type { CredentialVault } from "./types.js";

interface StoredCredential {
  readonly version: 1;
  readonly accessToken: string;
  readonly refreshToken: string | null;
  readonly expiresAt: string | null;
  readonly scope: string;
}

interface TokenResponse {
  readonly access_token?: string;
  readonly refresh_token?: string;
  readonly expires_in?: number;
  readonly scope?: string;
}

export interface LinearCredentialAccess {
  readonly ready: boolean;
  accessToken(reference: string): Promise<string | null>;
  delete(reference: string): Promise<void>;
}

export interface LinearOAuthOptions {
  readonly clientId: string;
  readonly clientSecret: string;
  readonly redirectUri: string;
  readonly scopes: readonly string[];
  /** Where PKCE state and the completed installation are retained. */
  readonly store: LinearInstallationOAuthPort;
  readonly cipher: EnvelopeCipher;
  readonly vault: CredentialVault;
  readonly provider: LinearProviderPort;
  readonly defaultTeamIds?: readonly string[];
  readonly allowAllPublicTeams?: boolean;
  readonly authorizeEndpoint?: string;
  readonly tokenEndpoint?: string;
  readonly fetch?: typeof fetch;
  readonly now?: () => Date;
}

export class LinearOAuthManager implements LinearCredentialAccess {
  readonly #clientId: string;
  readonly #clientSecret: string;
  readonly #redirectUri: string;
  readonly #scopes: readonly string[];
  readonly #store: LinearInstallationOAuthPort;
  readonly #cipher: EnvelopeCipher;
  readonly #vault: CredentialVault;
  readonly #provider: LinearProviderPort;
  readonly #defaultTeamIds: readonly string[];
  readonly #allowAllPublicTeams: boolean;
  readonly #authorizeEndpoint: string;
  readonly #tokenEndpoint: string;
  readonly #fetch: typeof fetch;
  readonly #now: () => Date;

  constructor(options: LinearOAuthOptions) {
    this.#clientId = options.clientId;
    this.#clientSecret = options.clientSecret;
    this.#redirectUri = options.redirectUri;
    this.#scopes = options.scopes;
    this.#store = options.store;
    this.#cipher = options.cipher;
    this.#vault = options.vault;
    this.#provider = options.provider;
    this.#defaultTeamIds = options.defaultTeamIds ?? [];
    this.#allowAllPublicTeams = options.allowAllPublicTeams ?? false;
    this.#authorizeEndpoint = options.authorizeEndpoint ?? "https://linear.app/oauth/authorize";
    this.#tokenEndpoint = options.tokenEndpoint ?? "https://api.linear.app/oauth/token";
    this.#fetch = options.fetch ?? fetch;
    this.#now = options.now ?? (() => new Date());
  }

  get ready(): boolean {
    return this.#vault.ready;
  }

  begin(returnTo = "/"): string {
    if (!returnTo.startsWith("/") || returnTo.startsWith("//")) {
      throw new RelayError("linear.auth.invalid_state", "The OAuth return path is invalid.", 400);
    }
    const state = randomBytes(32).toString("base64url");
    const verifier = randomBytes(48).toString("base64url");
    const challenge = createHash("sha256").update(verifier).digest("base64url");
    this.#store.saveOAuthState({
      stateDigest: sha256(state),
      verifierCiphertext: this.#cipher.encrypt(verifier),
      returnTo,
      expiresAt: new Date(this.#now().getTime() + 10 * 60_000).toISOString(),
    });
    const url = new URL(this.#authorizeEndpoint);
    url.searchParams.set("client_id", this.#clientId);
    url.searchParams.set("redirect_uri", this.#redirectUri);
    url.searchParams.set("response_type", "code");
    url.searchParams.set("scope", this.#scopes.join(" "));
    url.searchParams.set("actor", "app");
    url.searchParams.set("state", state);
    url.searchParams.set("code_challenge", challenge);
    url.searchParams.set("code_challenge_method", "S256");
    return url.toString();
  }

  async callback(code: string, state: string): Promise<string> {
    if (code.length === 0 || state.length === 0) throw new RelayError("linear.auth.invalid_state", "The OAuth callback is incomplete.", 400);
    const saved = this.#store.consumeOAuthState(sha256(state));
    if (saved === null) throw new RelayError("linear.auth.invalid_state", "The OAuth state is expired or already consumed.", 400);
    const verifier = this.#cipher.decrypt(saved.verifierCiphertext);
    const token = await this.#exchange({
      grant_type: "authorization_code",
      code,
      redirect_uri: this.#redirectUri,
      client_id: this.#clientId,
      client_secret: this.#clientSecret,
      code_verifier: verifier,
    });
    const viewer = await this.#provider.resolveViewer(token.accessToken);
    const credentialRef = `linear:${viewer.organizationId}:${viewer.appUserId}`;
    await this.#vault.put(credentialRef, JSON.stringify(token.credential));
    try {
      this.#store.install({
        organizationId: viewer.organizationId,
        appUserId: viewer.appUserId,
        credentialRef,
        teamIds: this.#defaultTeamIds,
        canAccessAllPublicTeams: this.#allowAllPublicTeams,
      });
    } catch (error) {
      await this.#vault.delete(credentialRef);
      throw error;
    }
    return saved.returnTo;
  }

  async accessToken(reference: string): Promise<string | null> {
    const raw = await this.#vault.get(reference);
    if (raw === null) return null;
    let credential: StoredCredential;
    try {
      credential = JSON.parse(raw) as StoredCredential;
    } catch {
      return null;
    }
    if (credential.version !== 1 || typeof credential.accessToken !== "string") return null;
    const expiresAt = credential.expiresAt === null ? Number.POSITIVE_INFINITY : Date.parse(credential.expiresAt);
    if (expiresAt > this.#now().getTime() + 60_000) return credential.accessToken;
    if (credential.refreshToken === null) return null;
    try {
      const refreshed = await this.#exchange({
        grant_type: "refresh_token",
        refresh_token: credential.refreshToken,
        client_id: this.#clientId,
        client_secret: this.#clientSecret,
      });
      await this.#vault.put(reference, JSON.stringify(refreshed.credential));
      return refreshed.accessToken;
    } catch {
      return null;
    }
  }

  async delete(reference: string): Promise<void> {
    await this.#vault.delete(reference);
  }

  async #exchange(parameters: Readonly<Record<string, string>>): Promise<{ readonly accessToken: string; readonly credential: StoredCredential }> {
    const response = await this.#fetch(this.#tokenEndpoint, {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams(parameters),
      signal: AbortSignal.timeout(5_000),
    });
    const value = await response.json() as TokenResponse;
    if (!response.ok || typeof value.access_token !== "string") {
      throw new RelayError("linear.auth.revoked", "Linear rejected the OAuth credential exchange.", 403);
    }
    const expiresAt = typeof value.expires_in === "number"
      ? new Date(this.#now().getTime() + value.expires_in * 1_000).toISOString()
      : null;
    const credential: StoredCredential = {
      version: 1,
      accessToken: value.access_token,
      refreshToken: value.refresh_token ?? null,
      expiresAt,
      scope: value.scope ?? this.#scopes.join(" "),
    };
    return { accessToken: value.access_token, credential };
  }
}
