import type { VerificationResult } from '../../types.js';
/**
 * Installs a specific Python package version in an isolated venv and
 * verifies that each requested symbol can still be imported.
 *
 * Strategy: For each symbol, run `python -c "from {pkg} import {symbol}"`
 * If the import fails, the symbol is INCOMPATIBLE.
 */
export declare function verifyImports(pkg: string, version: string, symbols: string[]): Promise<VerificationResult>;
//# sourceMappingURL=py-verify.d.ts.map