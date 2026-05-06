/**
 * Service for managing GitHub Actions workflow runs.
 *
 * Depends on GhClient (Dependency Inversion) — never calls `gh` directly.
 */

import { GhClient } from './gh-client.js';
import { Logger } from './logger.js';
import type { ApprovalResult, PullRequest, WorkflowRun, WorkflowRunsResponse } from './types.js';

export class WorkflowService {
    constructor(
        private readonly client: GhClient,
        private readonly logger: Logger,
    ) {}

    /** Get workflow runs with `action_required` conclusion for a given branch. */
    getPendingRuns(branch: string): WorkflowRun[] {
        const encoded = encodeURIComponent(branch);
        const response = this.client.execJson<WorkflowRunsResponse>([
            'api',
            `repos/${this.client.owner}/${this.client.repoName}/actions/runs?branch=${encoded}&per_page=10`,
        ]);

        if (!response) return [];
        return response.workflow_runs.filter((r) => r.conclusion === 'action_required');
    }

    /** Approve a single workflow run by re-running it as the authenticated user. */
    approveRun(runId: number): boolean {
        const result = this.client.exec(
            ['api', '--method', 'POST', `/repos/${this.client.owner}/${this.client.repoName}/actions/runs/${runId}/rerun`],
            { fatal: false },
        );
        return result.status === 0;
    }

    /** Approve all pending workflow runs for a list of PRs. */
    approveAll(prs: PullRequest[]): ApprovalResult[] {
        const results: ApprovalResult[] = [];

        for (const pr of prs) {
            this.logger.prHeader(pr.number, pr.title);

            if (this.client.isDryRun) {
                this.logger.dryRun(`Would check workflow runs for branch ${pr.headRefName}`);
                results.push({ pr, approved: 0, skipped: 0, failed: 0 });
                continue;
            }

            const pendingRuns = this.getPendingRuns(pr.headRefName);

            if (pendingRuns.length === 0) {
                this.logger.allClear();
                results.push({ pr, approved: 0, skipped: 0, failed: 0 });
                continue;
            }

            let approved = 0;
            let failed = 0;

            for (const run of pendingRuns) {
                this.logger.approving(run.name, run.id);
                const success = this.approveRun(run.id);
                if (success) approved++;
                else failed++;
            }

            results.push({ pr, approved, skipped: 0, failed });
        }

        return results;
    }
}
