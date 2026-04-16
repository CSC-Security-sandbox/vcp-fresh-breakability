import type { UsageRecord } from '../../types.js';
/**
 * Find all .go files in the repo that import the given module path,
 * then scan them for usage of the specified symbols.
 */
export declare function scanGoUsage(repoPath: string, modulePath: string, changedSymbols: string[]): Promise<UsageRecord[]>;
//# sourceMappingURL=go-usage-scan.d.ts.map