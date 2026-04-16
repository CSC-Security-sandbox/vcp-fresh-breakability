/**
 * Changelog Fetcher — three-source fallback chain.
 *
 * 1. PR body (Dependabot includes release notes excerpts)
 * 2. GitHub Releases API
 * 3. npm registry / unpkg
 *
 * If all three fail, returns "No changelog available".
 */
/**
 * Extract changelog content for a package upgrade.
 */
export declare function fetchChangelog(opts: {
    packageName: string;
    toVersion: string;
    prBody: string;
    repoOwner?: string;
    repoName?: string;
    githubToken?: string;
}): Promise<string>;
//# sourceMappingURL=changelog-fetcher.d.ts.map