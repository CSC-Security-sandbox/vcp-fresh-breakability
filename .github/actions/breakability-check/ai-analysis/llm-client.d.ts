/**
 * LLM Client — OpenAI-compatible wrapper with timeout + error handling.
 *
 * Uses the `openai` SDK pointing at the NetApp internal LLM proxy.
 * Hard 30-second timeout. AI never blocks the pipeline.
 */
import type { AiAnalysisConfig, BehavioralAnalysisResponse, ExploitabilityResponse } from './types.js';
export interface LlmCallResult<T> {
    data: T | null;
    inputTokens: number;
    outputTokens: number;
    durationMs: number;
    error: string | null;
}
export interface LlmClient {
    analyzeBehavior(systemPrompt: string, userPrompt: string): Promise<LlmCallResult<BehavioralAnalysisResponse>>;
    analyzeExploitability(systemPrompt: string, userPrompt: string): Promise<LlmCallResult<ExploitabilityResponse>>;
}
/**
 * Create a real LLM client backed by the OpenAI SDK.
 */
export declare function createLlmClient(config: AiAnalysisConfig): LlmClient;
//# sourceMappingURL=llm-client.d.ts.map