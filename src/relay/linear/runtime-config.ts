/**
 * The four databases the Linear channel opens. `channel` holds the inbox, ledgers and installation
 * stores, which scope their schema checks to disjoint table prefixes; the other three are
 * channel-neutral stores and cannot share a file.
 */
export interface LinearChannelDatabasePaths {
  /** The channel database: inbox, ledgers and installations. */
  readonly channel: string;
  readonly conversations: string;
  readonly runChain: string;
  readonly delivery: string;
}

/** The three `LINEAR_LOOP_NAMES` intervals, in milliseconds. */
export interface LinearTickIntervals {
  readonly ingress: number;
  readonly observe: number;
  readonly outbox: number;
}

export interface LinearOAuthConfig {
  readonly clientId: string;
  readonly clientSecret: string;
  readonly redirectUri: string;
}

export interface LinearRuntimeConfig {
  readonly databases: LinearChannelDatabasePaths;
  readonly oauth: LinearOAuthConfig;
  readonly webhookSecret: string;
  /** Configured Team ids, deduplicated. Empty when unset, which is valid and grants no Team by default. */
  readonly defaultTeamIds: readonly string[];
  /** Explicit public-Team consent. */
  readonly allowAllPublicTeams: boolean;
  readonly tickIntervals: LinearTickIntervals;
  /**
   * Minimum milliseconds between full store-integrity checks. Not a tick interval: it drives no loop.
   * See `../integrity-cadence.ts`.
   */
  readonly integrityCheckIntervalMs: number;
}


export const LINEAR_DEFAULT_TICK_INTERVAL_MS = 1_000;
