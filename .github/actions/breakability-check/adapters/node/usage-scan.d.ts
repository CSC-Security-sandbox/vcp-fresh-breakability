import type { UsageRecord } from '../../types.js';
/**
 * Maps @types/* packages to their runtime counterpart.
 * Returns null for built-ins like @types/node.
 */
export declare function resolveUsageScanTarget(packageName: string): string | null;
export declare function scanAllImports(fileContent: string, packageName: string): string[];
export declare function scanUsage(repoPath: string, packageName: string, changedSymbols: string[]): Promise<UsageRecord[]>;
/**
 * Fix A: Discover every symbol the codebase imports from a package.
 * Used by the untyped fallback flow when API extraction returns null.
 * Scans all files that import the package and collects all imported symbols.
 */
export declare function discoverAllImportedSymbols(repoPath: string, packageName: string): Promise<string[]>;
//# sourceMappingURL=usage-scan.d.ts.map