/**
 * Service for compacting Dependabot PRs into a single patch file.
 *
 * Fetches diffs from open Dependabot PRs and writes them to a local
 * patch file for an agent (or human) to review and apply.
 */

import { writeFileSync } from 'node:fs';
import { GhClient } from './gh-client.js';
import { Logger } from './logger.js';
import { PullRequestService } from './pull-request-service.js';
import type { CompactDependabotResult, PullRequest } from './types.js';

const DEFAULT_PATCH_FILE = 'dependabot-diffs.patch';

export class DependabotService {
    constructor(
        private readonly client: GhClient,
        private readonly prService: PullRequestService,
        private readonly logger: Logger,
    ) {}

    /**
     * Fetch diffs from all open Dependabot PRs and write a combined patch file.
     *
     * @param outputPath  Where to write the combined patch (default: `dependabot-diffs.patch`)
     * @returns Metadata about which PRs were included, or `null` in dry-run mode.
     */
    compact(outputPath: string = DEFAULT_PATCH_FILE): CompactDependabotResult | null {
        const prs = this.prService.listDependabotPRs();

        if (this.client.isDryRun && prs === null) {
            this.logger.dryRun(`Would fetch diffs for open Dependabot PRs and write to ${outputPath}`);
            return null;
        }

        if (!prs || prs.length === 0) {
            this.logger.noDependabotPRs();
            return null;
        }

        const sections: string[] = [];

        for (const pr of prs) {
            this.logger.fetchingDiff(pr.number, pr.title);
            const diff = this.fetchDiff(pr);
            if (diff) {
                sections.push(this.formatPatchSection(pr, diff));
            }
        }

        const combined = sections.join('\n');
        writeFileSync(outputPath, combined, 'utf-8');
        this.logger.patchWritten(outputPath, prs.length);

        return { prs, patchFile: outputPath };
    }

    /** Fetch the raw diff for a single PR. */
    private fetchDiff(pr: PullRequest): string | null {
        const result = this.client.exec(
            ['pr', 'diff', String(pr.number), '--repo', this.client.repoSlug],
            { fatal: false },
        );
        const output = result.stdout?.trim();
        return output || null;
    }

    /** Wrap a PR diff with a header comment for readability. */
    private formatPatchSection(pr: PullRequest, diff: string): string {
        const header = [
            `# ──────────────────────────────────────────────────────────────`,
            `# PR #${pr.number}: ${pr.title}`,
            `# Branch: ${pr.headRefName}`,
            `# URL: ${pr.url}`,
            `# ──────────────────────────────────────────────────────────────`,
        ].join('\n');

        return `${header}\n\n${diff}\n`;
    }
}
