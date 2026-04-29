/**
 * Service for generating a changelog of PR descriptions between the last
 * version bump and the current HEAD of main.
 *
 * Uses git log for commit history and `gh api` to resolve associated PRs.
 */

import { spawnSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { GhClient } from './gh-client.js';
import { Logger } from './logger.js';

export interface VersionDiffConfig {
    readonly repo: string;
    readonly dryRun: boolean;
    readonly output: string;
}

interface CommitInfo {
    readonly sha: string;
    readonly shortSha: string;
    readonly title: string;
}

interface PrInfo {
    readonly number: number;
    readonly title: string;
    readonly body: string | null;
}

export class VersionDiffService {
    private readonly client: GhClient;
    private readonly logger: Logger;
    private readonly config: VersionDiffConfig;

    private static readonly VERSION_FILE = 'src/dbt/adapters/fabricspark/__version__.py';

    constructor(client: GhClient, logger: Logger, config: VersionDiffConfig) {
        this.client = client;
        this.logger = logger;
        this.config = config;
    }

    /** Run the full changelog generation pipeline. */
    run(): void {
        const lastVersionCommit = this.findLastVersionCommit();
        if (!lastVersionCommit) {
            this.logger.error('Could not find any commit that modified __version__.py on main.');
            process.exit(1);
        }

        this.logger.info(`Last version bump commit: ${lastVersionCommit.substring(0, 7)}`);

        const commits = this.getCommitsSince(lastVersionCommit);
        if (commits.length === 0) {
            this.logger.info('No commits found since last version bump. Nothing to do.');
            return;
        }

        this.logger.info(`Found ${commits.length} commit(s) since last version bump.`);

        const entries = this.resolvePrDescriptions(commits);
        const markdown = this.formatMarkdown(entries);

        if (this.config.dryRun) {
            this.logger.dryRun(`Would write ${entries.length} entries to ${this.config.output}`);
            this.logger.info('\n' + markdown);
            return;
        }

        mkdirSync(dirname(this.config.output), { recursive: true });
        writeFileSync(this.config.output, markdown, 'utf-8');
        this.logger.info(`\n✓ Wrote changelog (${entries.length} entries) to ${this.config.output}`);
    }

    /** Find the last commit on main that modified __version__.py. */
    private findLastVersionCommit(): string | null {
        const result = spawnSync('git', ['log', '--format=%H', '-1', 'main', '--', VersionDiffService.VERSION_FILE], {
            encoding: 'utf-8',
            stdio: ['inherit', 'pipe', 'pipe'],
        });

        if (result.status !== 0 || !result.stdout.trim()) {
            return null;
        }

        return result.stdout.trim();
    }

    /** Get all commits between the version commit (exclusive) and HEAD of main. */
    private getCommitsSince(sinceSha: string): CommitInfo[] {
        const result = spawnSync('git', ['log', '--format=%H %s', `${sinceSha}..main`], {
            encoding: 'utf-8',
            stdio: ['inherit', 'pipe', 'pipe'],
        });

        if (result.status !== 0 || !result.stdout.trim()) {
            return [];
        }

        return result.stdout
            .trim()
            .split('\n')
            .map((line) => {
                const spaceIdx = line.indexOf(' ');
                const sha = line.substring(0, spaceIdx);
                const title = line.substring(spaceIdx + 1);
                return { sha, shortSha: sha.substring(0, 7), title };
            });
    }

    /** For each commit, find the associated PR via GitHub API. */
    private resolvePrDescriptions(commits: CommitInfo[]): Array<{ commit: CommitInfo; pr: PrInfo | null }> {
        const seen = new Set<number>();
        const entries: Array<{ commit: CommitInfo; pr: PrInfo | null }> = [];

        for (const commit of commits) {
            this.logger.info(`  → Resolving PR for ${commit.shortSha}: ${commit.title}`);

            const pr = this.fetchPrForCommit(commit.sha);

            if (pr && seen.has(pr.number)) {
                continue; // deduplicate
            }

            if (pr) {
                seen.add(pr.number);
            }

            entries.push({ commit, pr });
        }

        return entries;
    }

    /** Use gh api to find the PR associated with a commit. */
    private fetchPrForCommit(sha: string): PrInfo | null {
        const result = this.client.exec(
            ['api', `repos/${this.client.repoSlug}/commits/${sha}/pulls`, '--jq', '.[0] // empty'],
            { fatal: false },
        );

        if (result.status !== 0 || !result.stdout.trim()) {
            return null;
        }

        try {
            const data = JSON.parse(result.stdout) as { number: number; title: string; body: string | null };
            return { number: data.number, title: data.title, body: data.body };
        } catch {
            return null;
        }
    }

    /** Format the entries into a markdown changelog. */
    private formatMarkdown(entries: Array<{ commit: CommitInfo; pr: PrInfo | null }>): string {
        const sections = entries.map(({ commit, pr }) => {
            const heading = `# ${commit.shortSha}: ${commit.title}`;
            const body = pr?.body?.trim() || '_(No PR description found)_';
            return `${heading}\n\n${body}`;
        });

        return sections.join('\n\n---\n\n') + '\n';
    }
}
