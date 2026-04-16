import type { ExportMapResult } from '../../types.js';
/**
 * Extract the full API surface of a package at a given version.
 * Installs the package in a temp directory and uses the TS Compiler API.
 */
export declare function extractApi(pkg: string, version: string): Promise<ExportMapResult | null>;
/**
 * Extract API surface from an already-installed package directory.
 * Useful for testing with fixture packages.
 */
export declare function extractApiFromDir(pkg: string, installDir: string): ExportMapResult | null;
//# sourceMappingURL=ts-extractor.d.ts.map