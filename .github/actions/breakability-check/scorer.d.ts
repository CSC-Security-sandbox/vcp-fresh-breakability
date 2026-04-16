import type { Classification, DepType, ScoreInput, ScoreResult } from './types.js';
export declare function computeScore(input: ScoreInput): ScoreResult;
export declare function classify(score: ScoreResult, hasHardBreaks: boolean): Classification;
export declare function getLabel(classification: Classification, depType: DepType): string;
