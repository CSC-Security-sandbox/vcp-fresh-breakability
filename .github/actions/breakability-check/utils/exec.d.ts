export interface ExecResult {
    stdout: string;
    stderr: string;
    exitCode: number;
}
export declare function exec(command: string, args: string[], options?: {
    cwd?: string;
    timeout?: number;
    env?: Record<string, string>;
}): Promise<ExecResult>;
//# sourceMappingURL=exec.d.ts.map