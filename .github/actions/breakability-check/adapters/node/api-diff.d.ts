import type { ApiChange, ExportEntry, ExportMap } from '../../types.js';
/**
 * Classify the nature of a type change between old and new export entries.
 */
export declare function classifyTypeChange(oldEntry: ExportEntry, newEntry: ExportEntry): ApiChange['changeType'];
/**
 * Compare two ExportMaps and return a list of API changes.
 */
export declare function diffApis(oldExports: ExportMap, newExports: ExportMap): ApiChange[];
//# sourceMappingURL=api-diff.d.ts.map