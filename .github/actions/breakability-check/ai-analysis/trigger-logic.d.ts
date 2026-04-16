/**
 * AI Trigger Logic — decides whether AI should run for a given analysis.
 *
 * AI is selectively invoked to control cost. Clear LOW cases skip AI entirely.
 */
import type { TriggerContext } from './types.js';
export declare function shouldTriggerAi(ctx: TriggerContext): {
    trigger: boolean;
    reason: string | null;
};
/**
 * Check if changelog text contains breaking-change keywords.
 */
export declare function hasBreakingKeywords(changelog: string): boolean;
//# sourceMappingURL=trigger-logic.d.ts.map