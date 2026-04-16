/**
 * Evidence Validator — post-hoc validation of AI claims at zero LLM cost.
 *
 * Cross-checks AI-cited file paths and diff hunks against reality.
 * If >30% of claims are fabricated, downgrades confidence.
 */
import type { AiBehavioralChange, EvidenceValidationResult } from './types.js';
export declare function validateEvidence(aiResult: AiBehavioralChange[], repoFileList: string[], patchDiff: string): EvidenceValidationResult;
/**
 * Downgrade confidence levels by one tier when fabrication rate is high.
 * high → medium, medium → low, low → discard (filtered out).
 */
export declare function downgradeConfidence(changes: AiBehavioralChange[]): AiBehavioralChange[];
//# sourceMappingURL=evidence-validator.d.ts.map