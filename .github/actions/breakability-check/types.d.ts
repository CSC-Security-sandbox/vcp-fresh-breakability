export interface AnalysisResult {
    package: string;
    ecosystem: string;
    fromVersion: string;
    toVersion: string;
    semverBump: 'patch' | 'minor' | 'major' | 'pre-release' | 'unknown';
    depType: DepType;
    depRelation: DepRelation;
    securityUpdate: SecurityUpdateInfo | null;
    versionGap: number;
    hasCi: boolean;
    apiChanges: ApiChange[];
    verification: VerificationResult;
    usages: UsageRecord[];
    score: ScoreResult;
    classification: Classification;
    confidence: Confidence;
    adapterUsed: string;
    stageResults: StageResult[];
    timestamp: string;
    aiAnalysis?: AiAnalysisResultSummary;
}
/** Subset of AI result stored on AnalysisResult (avoids circular deps with ai-analysis/types.ts) */
export interface AiAnalysisResultSummary {
    triggered: boolean;
    triggerReason: string | null;
    behavioralChanges: Array<{
        description: string;
        severity: 'low' | 'medium' | 'high' | 'critical';
        affectedFiles: string[];
        diffEvidence: string;
        confidence: 'high' | 'medium' | 'low';
        reasoning: string;
    }>;
    exploitability: {
        reachability: 'REACHABLE' | 'POTENTIALLY_REACHABLE' | 'NOT_REACHABLE' | 'UNKNOWN';
        attackVector: string;
        evidenceChain: string[];
        affectedEntryPoints: string[];
        confidence: 'high' | 'medium' | 'low';
        reasoning: string;
    } | null;
    modelUsed: string;
    inputTokens: number;
    outputTokens: number;
    durationMs: number;
    validationResult: {
        totalClaims: number;
        verifiedClaims: number;
        fabricatedClaims: number;
        fabricationRate: number;
        confidenceAdjusted: boolean;
        details: string[];
    };
}
export type DepType = 'production' | 'dev' | 'peer' | 'optional' | 'unknown';
export type DepRelation = 'direct' | 'transitive' | 'unknown';
export type Classification = 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL' | 'INCONCLUSIVE';
export type Confidence = 'VERIFIED' | 'VERIFIED_EXISTENCE' | 'UNVERIFIED' | 'PARTIAL';
export interface SecurityUpdateInfo {
    isSecurity: boolean;
    cveIds: string[];
    cvssScore: number | null;
    severity: 'low' | 'moderate' | 'high' | 'critical' | null;
    advisoryUrl: string | null;
    vulnerableVersionRange: string | null;
    description: string | null;
}
export interface ApiChange {
    symbol: string;
    changeType: 'removed' | 'signature_changed' | 'type_changed' | 'return_type_changed' | 'default_changed' | 'added' | 'deprecated';
    oldDefinition?: string;
    newDefinition?: string;
    subpath?: string;
    isHardBreak: boolean;
}
export interface VerificationResult {
    tier: 1 | 2 | 3;
    verified: boolean;
    compatible: boolean | null;
    errors: VerificationError[];
    symbolResults: Map<string, 'COMPATIBLE' | 'INCOMPATIBLE' | 'UNVERIFIED'>;
}
export interface VerificationError {
    symbol: string;
    message: string;
    detail?: string;
}
export interface UsageRecord {
    file: string;
    line: number;
    symbol: string;
    usageType: 'DIRECT_CALL' | 'PROPERTY_ACCESS' | 'IMPORT_ONLY' | 'RE_EXPORT' | 'NOT_USED';
    isReExport: boolean;
    reExportConsumers?: string[];
}
export interface ScoreResult {
    total: number;
    hardBreakScore: number;
    softSignalScore: number;
    breakdown: ScoreBreakdownEntry[];
}
export interface ScoreBreakdownEntry {
    signal: string;
    points: number;
    category: 'hard_break' | 'soft_signal' | 'semver';
    details: string;
}
export interface ExportEntry {
    name: string;
    type: string;
    signatures?: string[];
    subpath?: string;
}
export type ExportMap = Map<string, ExportEntry>;
export interface ExportMapResult {
    exports: ExportMap;
    confidence: 'FULL' | 'HOLLOW' | 'PARTIAL';
}
export interface EcosystemAdapter {
    id: string;
    detect(prInfo: PrInfo): boolean;
    extractApi(pkg: string, version: string): Promise<ExportMapResult | null>;
    diffApis(oldExports: ExportMap, newExports: ExportMap): ApiChange[];
    verify(usedSymbols: string[], pkg: string, newVersion: string): Promise<VerificationResult>;
    scanUsage(repoPath: string, pkg: string, changedSymbols: string[]): Promise<UsageRecord[]>;
    getSourceDiff?(pkg: string, oldVer: string, newVer: string): Promise<string | null>;
}
export interface PrInfo {
    packages: PackageUpdate[];
    isGrouped: boolean;
    prNumber: number;
    prTitle: string;
    prBody: string;
    repoPath: string;
    repoOwner: string;
    repoName: string;
}
export interface PackageUpdate {
    name: string;
    ecosystem: string;
    fromVersion: string;
    toVersion: string;
    depType: DepType;
    depRelation: DepRelation;
    securityUpdate: SecurityUpdateInfo | null;
    versionGap: number;
}
export interface SemverResult {
    bump: 'patch' | 'minor' | 'major' | 'pre-release' | 'unknown';
    baseScore: number;
}
export interface ScoreInput {
    semver: SemverResult;
    changes: ApiChange[];
    usages: UsageRecord[];
    depType: DepType;
}
export interface StageResult {
    stage: string;
    success: boolean;
    durationMs: number;
    error?: string;
}
export interface PrMeta {
    repo: string;
    prNumber: number;
    prTitle?: string;
}
export interface AccuracyLogEntry {
    timestamp: string;
    repo: string;
    prNumber: number;
    prTitle: string;
    package: string;
    fromVersion: string;
    toVersion: string;
    ecosystem: string;
    depType: DepType;
    depRelation: DepRelation;
    isSecurity: boolean;
    cveIds: string[];
    cvssScore: number | null;
    versionGap: number;
    hasCi: boolean;
    score: number;
    classification: string;
    label: string;
    confidence: string;
    hardBreakCount: number;
    softSignalCount: number;
    verificationTier: number;
    verificationRate: string;
    adapterUsed: string;
    apiChangesDetected: number;
    usageFilesFound: number;
}
