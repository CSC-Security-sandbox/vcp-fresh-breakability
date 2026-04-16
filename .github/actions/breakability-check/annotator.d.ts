import type { AnalysisResult } from './types.js';
export declare const COMMENT_MARKER = "<!-- breakability-check -->";
/**
 * Generate a single consolidated PR comment for all analyzed packages.
 * This is the ONLY comment posted per PR — no per-package spam.
 */
export declare function generateConsolidatedComment(results: AnalysisResult[]): string;
/**
 * Decide which single label to apply to the PR.
 * Uses the worst-case verdict across all packages — label matches what the comment says.
 */
export declare function getConsolidatedLabel(results: AnalysisResult[]): string;
/**
 * Should we post a comment at all?
 */
export declare function shouldPostComment(_results: AnalysisResult[]): boolean;
/** @deprecated Use generateConsolidatedComment([result]) */
export declare function generateComment(result: AnalysisResult): string;
/** @deprecated Use getConsolidatedLabel([result]) */
export declare function getLabel(result: AnalysisResult): string;
