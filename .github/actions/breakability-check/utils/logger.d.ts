export type LogLevel = 'debug' | 'info' | 'warn' | 'error';
export declare function setLogLevel(level: LogLevel): void;
export declare function log(level: LogLevel, stage: string, message: string, data?: Record<string, unknown>): void;
export declare function appendJsonl(filePath: string, entry: Record<string, unknown>): void;
//# sourceMappingURL=logger.d.ts.map