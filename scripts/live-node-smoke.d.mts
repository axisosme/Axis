export type LiveNodeSmokeStatus = "passed" | "failed" | "unverified";

export interface LiveNodeSmokeCheck {
  readonly name: string;
  readonly status: LiveNodeSmokeStatus;
  readonly safeCode: string;
  readonly evidence?: unknown;
}

export interface LiveNodeSmokeReceipt {
  readonly contractVersion: "axis.node.live-smoke.receipt.v1";
  readonly mode: "configuration-only" | "live-read-only" | "refused";
  readonly result: LiveNodeSmokeStatus;
  readonly mutationAttempted: false;
  readonly reviewedCommit?: string;
  readonly checks: readonly LiveNodeSmokeCheck[];
}

export interface PlaceholderService {
  readonly hostRoot: string;
  readonly serviceLabel: string;
}

export const placeholderConfiguration: string;
export function placeholderRuntimeConfigurations(): {
  readonly controller: import("../src/configuration.js").RelayConfiguration;
  readonly node: import("../src/configuration.js").NodeConfiguration;
  readonly first: import("../src/configuration.js").WorkerConfiguration;
  readonly second: import("../src/configuration.js").WorkerConfiguration;
};

export const placeholderInstallation: Readonly<{
  controller: PlaceholderService;
  node: PlaceholderService;
  workerA: PlaceholderService;
  workerB: PlaceholderService;
}>;

export function validatePlaceholderInstallation(): {
  readonly services: number;
  readonly workers: number;
  readonly distinctHostRoots: number;
  readonly distinctDatabases: number;
  readonly distinctAuthorities: number;
};

export function runLiveNodeSmoke(options?: Readonly<{
  args?: readonly string[];
  env?: Readonly<Record<string, string | undefined>>;
  fetch?: typeof globalThis.fetch;
}>): Promise<{ readonly exitCode: number; readonly receipt: LiveNodeSmokeReceipt }>;
