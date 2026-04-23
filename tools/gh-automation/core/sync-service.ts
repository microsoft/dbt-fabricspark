/**
 * Service for syncing Copilot PR branches with main.
 *
 * Checks whether each PR's branch is behind main, and if so,
 * posts a comment telling the Copilot agent to merge and re-test.
 */

import { GhClient } from './gh-client.js';
import { Logger } from './logger.js';
import type { CompareResponse, PullRequest, SyncResult } from './types.js';

const SYNC_COMMENT = [
    '@copilot There have been updates to main.',
    'Merge them into your branch, restart your task evaluation',
    'and keep trying until CI tests run green.',
].join(' ');

export class SyncService {
    constructor(
        private readonly client: GhClient,
        private readonly logger: Logger,
    ) {}

    /** Check how far behind main a branch is. Returns 0 if up to date. */
    getBehindCount(branch: string): number {
        const encoded = encodeURIComponent(branch);
        const response = this.client.execJson<CompareResponse>([
            'api',
            `repos/${this.client.owner}/${this.client.repoName}/compare/main...${encoded}`,
        ]);

        if (!response) return 0;
        return response.behind_by;
    }

    /** Sync all PRs: check each branch against main and comment if behind. */
    syncAll(prs: PullRequest[]): SyncResult[] {
        const results: SyncResult[] = [];
        let commented = 0;
        let upToDate = 0;
        let failed = 0;

        for (const pr of prs) {
            this.logger.prHeader(pr.number, pr.title);

            if (this.client.isDryRun) {
                this.logger.dryRun(`Would check if ${pr.headRefName} is behind main and comment if so`);
                results.push({ pr, behindBy: 0, commented: false });
                continue;
            }

            const behindBy = this.getBehindCount(pr.headRefName);

            if (behindBy === 0) {
                this.logger.upToDate(pr.headRefName);
                results.push({ pr, behindBy: 0, commented: false });
                upToDate++;
                continue;
            }

            this.logger.behindMain(pr.headRefName, behindBy);
            const success = this.client.postComment(pr.number, SYNC_COMMENT);

            if (success) {
                this.logger.commentPosted(pr.number);
                commented++;
            } else {
                this.logger.commentFailed(pr.number);
                failed++;
            }

            results.push({ pr, behindBy, commented: success });
        }

        this.logger.syncSummary(commented, upToDate, failed);
        return results;
    }
}
