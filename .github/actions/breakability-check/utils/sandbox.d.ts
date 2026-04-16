import { type ExecResult } from './exec.js';
export interface SandboxOptions {
    workDir: string;
    timeout?: number;
    networkAccess?: boolean;
}
export declare function runInSandbox(script: string, options: SandboxOptions): Promise<ExecResult>;
//# sourceMappingURL=sandbox.d.ts.map