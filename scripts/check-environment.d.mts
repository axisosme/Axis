export const REQUIRED_CONTRACTS_VERSION: "1.0.0";

export interface CheckEnvironmentResult {
  readonly ok: boolean;
  readonly typescriptVersion: string | null;
  readonly contractsVersion: string | null;
  readonly problems: readonly string[];
}

export function checkEnvironment(cwd: string, requiredContractsVersion?: string): CheckEnvironmentResult;
