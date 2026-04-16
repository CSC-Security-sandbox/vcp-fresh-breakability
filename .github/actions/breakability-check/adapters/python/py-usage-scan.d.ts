import type { UsageRecord } from '../../types.js';
/**
 * Parse all symbols imported from the target package in a Python file.
 */
export declare function scanAllImports(fileContent: string, packageName: string): string[];
export declare function scanUsage(repoPath: string, packageName: string, changedSymbols: string[]): Promise<UsageRecord[]>;
/**
 * Discover every symbol the codebase imports from a Python package.
 */
export declare function discoverAllImportedSymbols(repoPath: string, packageName: string): Promise<string[]>;
//# sourceMappingURL=py-usage-scan.d.ts.map