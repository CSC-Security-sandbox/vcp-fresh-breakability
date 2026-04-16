import type { VerificationResult } from '../../types.js';
/**
 * Installs a specific package version in an isolated temp directory and
 * verifies that each requested symbol still exists in the new version.
 *
 * Strategy:
 *   1. ESM probe — write a .mjs that does `import { symbol } from 'pkg'`
 *      and run with node --experimental-vm-modules.
 *   2. CJS fallback — require() the package, check Object.keys().
 *
 * Returns per-symbol COMPATIBLE / INCOMPATIBLE / UNVERIFIED status.
 */
export declare function verifyImports(pkg: string, version: string, symbols: string[]): Promise<VerificationResult>;
//# sourceMappingURL=verify-imports.d.ts.map