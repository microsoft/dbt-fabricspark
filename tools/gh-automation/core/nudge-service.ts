/**
 * Service for nudging Copilot agents on failed CI runs.
 *
 * Detects failed/cancelled/timed-out workflow runs on Copilot PRs
 * and posts a comment telling the agent to read the logs and fix.
 */

import { GhClient } from './gh-client.js';
import { Logger } from './logger.js';
import type { NudgeResult, PullRequest, WorkflowRun, WorkflowRunsResponse } from './types.js';

const NUDGE_CONCLUSIONS = new Set(['failure', 'cancelled', 'timed_out']);

function buildNudgeComment(run: WorkflowRun): string {
    return [
        '@copilot The latest CI run failed:',
        '',
        `- [${run.name}](${run.html_url}) — \`${run.conclusion}\``,
        '',
        'Read the logs, fix your code, and push again. Keep trying until CI is green.',
    ].join('\n');
}

export class NudgeService {
    constructor(
        private readonly client: GhClient,
        private readonly logger: Logger,
    ) {}

    /** Get the latest failed/cancelled/timed-out workflow run for a branch. */
    getLatestFailedRun(branch: string): WorkflowRun | null {
        const encoded = encodeURIComponent(branch);
        const response = this.client.execJson<WorkflowRunsResponse>([
            'api',
            `repos/${this.client.owner}/${this.client.repoName}/actions/runs?branch=${encoded}&per_page=100`,
        ]);

        if (!response) return null;
        // API returns runs sorted by created_at desc; first match is the latest.
        return response.workflow_runs.find(
            (r) => r.conclusion !== null && NUDGE_CONCLUSIONS.has(r.conclusion),
        ) ?? null;
    }

    /** Nudge all PRs with failed CI runs. */
    nudgeAll(prs: PullRequest[]): NudgeResult[] {
        const results: NudgeResult[] = [];
        let nudged = 0;
        let clean = 0;
        let failed = 0;

        for (const pr of prs) {
            this.logger.prHeader(pr.number, pr.title);

            if (this.client.isDryRun) {
                this.logger.dryRun(`Would check failed CI runs for branch ${pr.headRefName} and nudge if any`);
                results.push({ pr, failedRuns: 0, commented: false });
                continue;
            }

            const latestFailedRun = this.getLatestFailedRun(pr.headRefName);

            if (!latestFailedRun) {
                this.logger.noCiFailures();
                results.push({ pr, failedRuns: 0, commented: false });
                clean++;
                continue;
            }

            this.logger.ciFailure(latestFailedRun.name, latestFailedRun.id, latestFailedRun.conclusion!);

            const comment = buildNudgeComment(latestFailedRun);
            const success = this.client.postComment(pr.number, comment);

            if (success) {
                this.logger.nudgePosted(pr.number, 1);
                nudged++;
            } else {
                this.logger.nudgeFailed(pr.number);
                failed++;
            }

            results.push({ pr, failedRuns: 1, commented: success });
        }

        this.logger.nudgeSummary(nudged, clean, failed);
        return results;
    }
}
