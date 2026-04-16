import type { VerificationResult } from '../../types.js';
/**
 * Verify that the given symbols compile against a new version of a Go module.
 *
 * Strategy: create a temp Go project that imports the module at the target
 * version and references each symbol. Run `go build ./...` — if it compiles,
 * the symbols are compatible.
 */
export declare function verifyGoImports(modulePath: string, version: string, symbols: string[]): Promise<VerificationResult>;
//# sourceMappingURL=go-verify.d.ts.map