import type { AnalysisResult } from './types.js';
export interface AnalyzePackageInput {
    packageName: string;
    ecosystem: string;
    fromVersion: string;
    toVersion: string;
    depType: AnalysisResult['depType'];
    depRelation: AnalysisResult['depRelation'];
    securityUpdate: AnalysisResult['securityUpdate'];
    versionGap: number;
    repoPath: string;
    hasCi: boolean;
    excludePackages?: string[];
}
export declare function analyzePackage(input: AnalyzePackageInput): Promise<AnalysisResult>;
