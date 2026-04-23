/**
 * Low-level wrapper around the `gh` CLI.
 *
 * Encapsulates process spawning, dry-run handling, and JSON parsing
 * so that higher-level services never touch `child_process` directly.
 */

import { spawnSync, type SpawnSyncReturns } from 'node:child_process';
import type { GhAutomationConfig } from './types.js';
import { Logger } from './logger.js';

export interface ExecOptions {
    fatal?: boolean;
}

export class GhClient {
    private readonly repo: string;
    private readonly dryRun: boolean;
    private readonly logger: Logger;

    constructor(config: GhAutomationConfig, logger: Logger) {
        this.repo = config.repo;
        this.dryRun = config.dryRun;
        this.logger = logger;
    }

    get repoSlug(): string {
        return this.repo;
    }

    get owner(): string {
        return this.repo.split('/')[0];
    }

    get repoName(): string {
        return this.repo.split('/')[1];
    }

    get isDryRun(): boolean {
        return this.dryRun;
    }

    /** Execute a `gh` CLI command and return the raw result. */
    exec(args: string[], { fatal = true }: ExecOptions = {}): SpawnSyncReturns<string> {
        const cmd = ['gh', ...args];

        if (this.dryRun) {
            this.logger.dryRun(cmd.join(' '));
            return {
                status: 0,
                stdout: '',
                stderr: '',
                pid: 0,
                output: ['', ''],
                signal: null,
            };
        }

        const result = spawnSync('gh', args, {
            encoding: 'utf-8',
            stdio: ['inherit', 'pipe', 'pipe'],
        });

        if (result.status !== 0) {
            const stderr = result.stderr?.trim();
            const stdout = result.stdout?.trim();
            const details = stderr || stdout || `(exit code ${result.status})`;
            const msg = `gh command failed: ${cmd.join(' ')}\n         ${details}`;
            if (fatal) {
                this.logger.error(msg);
                process.exit(1);
            }
            this.logger.warn(msg);
        }

        return result;
    }

    /** Execute a `gh` CLI command and parse the JSON output. Returns `null` in dry-run mode. */
    execJson<T>(args: string[]): T | null {
        if (this.dryRun) {
            this.exec(args);
            return null;
        }
        const result = this.exec(args);
        return JSON.parse(result.stdout) as T;
    }

    /** Post a comment on a pull request. */
    postComment(prNumber: number, body: string): boolean {
        const result = this.exec(
            ['pr', 'comment', String(prNumber), '--repo', this.repo, '--body', body],
            { fatal: false },
        );
        return result.status === 0;
    }
}
