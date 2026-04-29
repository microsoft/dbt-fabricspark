#!/usr/bin/env tsx
/**
 * CLI entry point: generate a changelog of PR descriptions since the last version bump.
 *
 * Finds the last commit that modified __version__.py on main, lists all
 * commits since then, fetches associated PR descriptions via the GitHub API,
 * and writes a markdown changelog.
 *
 * Usage:
 *   npx tsx tools/gh-automation/get-version-diffs.ts
 *   npx tsx tools/gh-automation/get-version-diffs.ts --dry-run
 *   npx tsx tools/gh-automation/get-version-diffs.ts --output custom-path.md
 */

import { Command } from 'commander';
import { Logger } from './core/logger.js';
import { GhClient } from './core/gh-client.js';
import { VersionDiffService } from './core/version-diff-service.js';

interface GetVersionDiffsOptions {
    readonly repo: string;
    readonly dryRun: boolean;
    readonly output: string;
    // Required by GhAutomationConfig shape used in GhClient
    readonly watch: boolean;
    readonly sleep: string;
}

const program = new Command();

program
    .name('get-version-diffs')
    .description('Generate a changelog of PR descriptions since the last version bump')
    .option('--dry-run', 'Print actions without executing them', false)
    .option('--repo <owner/repo>', 'GitHub repository', 'microsoft/dbt-fabricspark')
    .option('--output <path>', 'Output markdown file path', '.temp/VERSION_CHANGELOG_FULL.md')
    .action((opts: GetVersionDiffsOptions) => {
        const logger = new Logger();
        const client = new GhClient(opts, logger);
        const service = new VersionDiffService(client, logger, {
            repo: opts.repo,
            dryRun: opts.dryRun,
            output: opts.output,
        });

        service.run();
    });

program.parse();
