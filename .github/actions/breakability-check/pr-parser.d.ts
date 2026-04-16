import type { PrInfo, DepType, DepRelation, SecurityUpdateInfo } from './types.js';
interface BranchParseResult {
    ecosystem: string;
    name: string;
    toVersion: string;
}
export declare function parseBranchName(branchRef: string): BranchParseResult | null;
export declare function parseManifestDiff(diff: string, ecosystem: string): Map<string, {
    from: string;
    to: string;
}>;
export declare function parseLockfileDiff(diff: string, ecosystem: string): Map<string, {
    from: string;
    to: string;
}>;
export declare function resolveVersions(pkgName: string, branchVersion: string | undefined, manifestVersions: Map<string, {
    from: string;
    to: string;
}>, lockfileVersions: Map<string, {
    from: string;
    to: string;
}>): {
    fromVersion: string;
    toVersion: string;
};
export declare function detectDepType(packageName: string, manifestDiff: string, ecosystem: string, packageJsonContent?: Record<string, unknown>): DepType;
export declare function detectDepRelation(packageName: string, manifestDiff: string, lockfileDiff: string, ecosystem: string): DepRelation;
export declare function detectSecurityUpdate(_prTitle: string, prBody: string, _packageName: string, _toVersion: string, githubToken?: string): Promise<SecurityUpdateInfo | null>;
export declare function calculateVersionGap(packageName: string, fromVersion: string, toVersion: string, ecosystem: string, fetchFn?: typeof fetch): Promise<number>;
export declare function detectCi(repoPath: string): boolean;
export interface ParsePrInput {
    headRef: string;
    prNumber: number;
    prTitle: string;
    prBody: string;
    diff: string;
    repoPath: string;
    repoOwner: string;
    repoName: string;
    packageJsonContent?: Record<string, unknown>;
    githubToken?: string;
}
export declare function parsePr(input: ParsePrInput): Promise<PrInfo>;
export declare function parsePrBranch(pr: {
    head: {
        ref: string;
    };
}): Pick<PrInfo, 'packages'>;
export declare function parsePrWithLockfile(manifestDiff: string, lockfileDiff: string, ecosystem: string): Map<string, {
    from: string;
    to: string;
}>;
export {};
