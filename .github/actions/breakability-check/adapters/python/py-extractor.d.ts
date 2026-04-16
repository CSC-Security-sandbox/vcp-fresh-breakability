import type { ExportMapResult } from '../../types.js';
/**
 * Extract the public API surface of a Python package at a given version.
 * Creates an isolated venv, installs the package, and inspects it.
 */
export declare function extractApi(pkg: string, version: string): Promise<ExportMapResult | null>;
//# sourceMappingURL=py-extractor.d.ts.map