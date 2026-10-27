export interface CheckedSearchFileTarget { readonly file: string; }
export interface CheckedSearchRefTarget { readonly ref: string; readonly path: string; readonly cwd?: string; }
export type CheckedSearchTarget = CheckedSearchFileTarget | CheckedSearchRefTarget;

export function readTargetText(target: CheckedSearchTarget): string;
export function countOccurrences(text: string, needle: string): number;
export function checkedSearch(input: { readonly target: CheckedSearchTarget; readonly pattern: string; readonly anchor: string }): {
  readonly patternCount: number;
  readonly anchorCount: number;
  readonly proven: true;
};
