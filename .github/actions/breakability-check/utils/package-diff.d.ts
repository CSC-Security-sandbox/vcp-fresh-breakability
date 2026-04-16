/**
 * Fetch the diff between two versions of an npm package using `npm diff`.
 * Returns the unified diff string, or empty string on failure.
 */
export declare function fetchPackageDiff(packageName: string, fromVersion: string, toVersion: string): Promise<string>;
//# sourceMappingURL=package-diff.d.ts.map