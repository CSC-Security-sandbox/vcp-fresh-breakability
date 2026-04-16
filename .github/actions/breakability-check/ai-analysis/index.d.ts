/**
 * AI Analysis Orchestrator — Stage 6.5
 *
 * Decides whether to trigger AI, builds prompts, calls LLM,
 * validates evidence, and returns the result.
 *
 * AI runs in parallel with the deterministic pipeline.
 * AI never blocks the pipeline — on failure, returns a no-op result.
 */
import type { AiAnalysisConfig, AiAnalysisInput, AiAnalysisResult, TriggerContext } from './types.js';
import { shouldTriggerAi, hasBreakingKeywords } from './trigger-logic.js';
import type { LlmClient } from './llm-client.js';
export type { AiAnalysisConfig, AiAnalysisInput, AiAnalysisResult };
export { shouldTriggerAi, hasBreakingKeywords };
/**
 * Empty result returned when AI is not triggered or fails.
 */
export declare function emptyAiResult(reason?: string): AiAnalysisResult;
/**
 * Run AI analysis. This is the main entry point called from the pipeline.
 *
 * @param config  AI configuration (proxy URL, key, model, etc.)
 * @param input   Evidence gathered by the deterministic pipeline
 * @param triggerCtx  Context for trigger decision
 * @param llmClientOverride  Optional mock client for testing
 */
export declare function runAiAnalysis(config: AiAnalysisConfig, input: AiAnalysisInput, triggerCtx: TriggerContext, llmClientOverride?: LlmClient): Promise<AiAnalysisResult>;
//# sourceMappingURL=index.d.ts.map