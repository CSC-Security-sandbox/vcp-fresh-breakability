/**
 * Cursor CLI Agent integration — agentic AI analysis for breakability.
 *
 * Instead of a single LLM prompt/response, this spawns a Cursor agent
 * that can read files, run commands, search code, and iterate.
 * Works across ALL ecosystems — the agent figures out how to analyze each one.
 *
 * Requires: CURSOR_API_KEY environment variable.
 *
 * Install on GH Actions: `curl https://cursor.com/install -fsS | bash`
 * Binary resolves via PATH (after install adds $HOME/.cursor/bin to PATH)
 * or via CURSOR_AGENT_PATH env var for custom installations.
 */
import type { AiAnalysisInput, AiAnalysisResult } from './types.js';
export declare function runCursorAgent(input: AiAnalysisInput, repoPath: string, model?: string): Promise<AiAnalysisResult>;
//# sourceMappingURL=cursor-agent.d.ts.map