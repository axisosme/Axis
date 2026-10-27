export interface MergedBySymbolInput {
  readonly symbol: string;
  readonly sourceRef: string;
  readonly targetRef: string;
  readonly cwd?: string;
}
export interface MergedBySymbolResult {
  readonly symbol: string;
  readonly sourceRef: string;
  readonly sourceOccurrences: number;
  readonly sourceFiles: readonly string[];
  readonly targetRef: string;
  readonly targetOccurrences: number;
  readonly targetFiles: readonly string[];
  readonly verdict: "present_in_target" | "absent_from_target";
}

export function mergedBySymbol(input: MergedBySymbolInput): MergedBySymbolResult;
