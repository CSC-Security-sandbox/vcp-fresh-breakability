import type { UsageRecord } from '../types.js';
/**
 * Read code snippets around usage locations.
 * Returns ~5 lines before and after each import, max 50 lines per file.
 */
export declare function readUsageSnippets(repoPath: string, usages: UsageRecord[]): Array<{
    path: string;
    importedSymbols: string[];
    relevantSnippet: string;
}>;
//# sourceMappingURL=snippet-reader.d.ts.map