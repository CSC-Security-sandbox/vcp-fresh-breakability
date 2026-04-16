import type { ExportMapResult } from '../../types.js';
/**
 * Initialize a temp Go module, add the target module as a dependency,
 * then run `go doc -all` to extract its exported symbols.
 */
export declare function extractGoApi(modulePath: string, version: string): Promise<ExportMapResult | null>;
//# sourceMappingURL=go-extractor.d.ts.map