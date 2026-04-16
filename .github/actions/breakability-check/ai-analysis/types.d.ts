/**
 * AI Behavioral Analysis — Types
 *
 * All types for the AI layer (Stage 6.5).
 * AI runs in parallel with the deterministic pipeline and produces
 * a separate section in the PR comment.
 */
import type { ApiChange, SecurityUpdateInfo } from '../types.js';
export interface AiAnalysisConfig {
    enabled: boolean;
    proxyUrl: string;
    proxyKey: string;
    model: string;
    exploitabilityCheck: boolean;
    maxTokensPerReview: number;
    useCursorAgent: boolean;
    cursorAgentModel: string;
    repoPath: string;
}
export interface AiAnalysisInput {
    packageName: string;
    fromVersion: string;
    toVersion: string;
    semverBump: string;
    patchDiff: string;
    changelog: string;
    usageFiles: UsageFileInfo[];
    apiChanges: ApiChange[];
    securityUpdate: SecurityUpdateInfo | null;
    repoFileList: string[];
}
export interface UsageFileInfo {
    path: string;
    importedSymbols: string[];
    relevantSnippet: string;
}
export interface AiBehavioralChange {
    description: string;
    severity: 'low' | 'medium' | 'high' | 'critical';
    affectedFiles: string[];
    diffEvidence: string;
    confidence: 'high' | 'medium' | 'low';
    reasoning: string;
}
export interface AiExploitabilityResult {
    reachability: 'REACHABLE' | 'POTENTIALLY_REACHABLE' | 'NOT_REACHABLE' | 'UNKNOWN';
    attackVector: string;
    evidenceChain: string[];
    affectedEntryPoints: string[];
    confidence: 'high' | 'medium' | 'low';
    reasoning: string;
}
export interface AiAnalysisResult {
    triggered: boolean;
    triggerReason: string | null;
    behavioralChanges: AiBehavioralChange[];
    exploitability: AiExploitabilityResult | null;
    modelUsed: string;
    inputTokens: number;
    outputTokens: number;
    durationMs: number;
    validationResult: EvidenceValidationResult;
}
export interface EvidenceValidationResult {
    totalClaims: number;
    verifiedClaims: number;
    fabricatedClaims: number;
    fabricationRate: number;
    confidenceAdjusted: boolean;
    details: string[];
}
export interface TriggerContext {
    semverBump: string;
    apiChangeCount: number;
    hasSecurityUpdate: boolean;
    classification: string;
    changelogHasBreakingKeyword: boolean;
}
export interface BehavioralAnalysisResponse {
    behavioral_changes: Array<{
        description: string;
        severity: 'low' | 'medium' | 'high' | 'critical';
        affected_files: string[];
        diff_evidence: string;
        confidence: 'high' | 'medium' | 'low';
        reasoning: string;
    }>;
}
export interface ExploitabilityResponse {
    reachability: 'REACHABLE' | 'POTENTIALLY_REACHABLE' | 'NOT_REACHABLE' | 'UNKNOWN';
    attack_vector: string;
    evidence_chain: string[];
    affected_entry_points: string[];
    confidence: 'high' | 'medium' | 'low';
    reasoning: string;
}
//# sourceMappingURL=types.d.ts.map