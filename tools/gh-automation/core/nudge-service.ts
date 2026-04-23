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

function buildNudgeComment(failedRuns: WorkflowRun[]): string {
    const runLines = failedRuns.map(
        (r) => `- [${r.name}](${r.html_url}) — \`${r.conclusion}\``,
    );

    return [
        '@copilot The following CI run(s) failed:',
        '',
        ...runLines,
        '',
        'Read the logs, fix your code, and push again. Keep trying until CI is green.',
    ].join('\n');
}

export class NudgeService {
    constructor(
        private readonly client: GhClient,
        private readonly logger: Logger,
    ) {}

    /** Get failed/cancelled/timed-out workflow runs for a branch. */
    getFailedRuns(branch: string): WorkflowRun[] {
        const encoded = encodeURIComponent(branch);
        const response = this.client.execJson<WorkflowRunsResponse>([
            'api',
            `repos/${this.client.owner}/${this.client.repoName}/actions/runs?branch=${encoded}&per_page=100`,
        ]);

        if (!response) return [];
        return response.workflow_runs.filter(
            (r) => r.conclusion !== null && NUDGE_CONCLUSIONS.has(r.conclusion),
        );
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

            const failedRuns = this.getFailedRuns(pr.headRefName);

            if (failedRuns.length === 0) {
                this.logger.noCiFailures();
                results.push({ pr, failedRuns: 0, commented: false });
                clean++;
                continue;
            }

            for (const run of failedRuns) {
                this.logger.ciFailure(run.name, run.id, run.conclusion!);
            }

            const comment = buildNudgeComment(failedRuns);
            const success = this.client.postComment(pr.number, comment);

            if (success) {
                this.logger.nudgePosted(pr.number, failedRuns.length);
                nudged++;
            } else {
                this.logger.nudgeFailed(pr.number);
                failed++;
            }

            results.push({ pr, failedRuns: failedRuns.length, commented: success });
        }

        this.logger.nudgeSummary(nudged, clean, failed);
        return results;
    }
}
