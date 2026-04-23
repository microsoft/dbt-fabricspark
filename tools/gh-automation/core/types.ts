/** Shared interfaces for gh-automation tooling. */

// ── Configuration ──────────────────────────────────────────────────────────────

export interface GhAutomationConfig {
    readonly repo: string;
    readonly dryRun: boolean;
    readonly watch: boolean;
    readonly sleep: string;
}

// ── GitHub domain models ───────────────────────────────────────────────────────

export interface PullRequest {
    readonly number: number;
    readonly title: string;
    readonly url: string;
    readonly headRefName: string;
}

export interface PullRequestWithAuthor extends PullRequest {
    readonly author: { readonly login: string };
}

export interface WorkflowRun {
    readonly id: number;
    readonly name: string;
    readonly status: string;
    readonly conclusion: string | null;
    readonly html_url: string;
    readonly head_branch: string;
}

export interface WorkflowRunsResponse {
    readonly workflow_runs: readonly WorkflowRun[];
}

// ── Results ────────────────────────────────────────────────────────────────────

export interface ApprovalResult {
    readonly pr: PullRequest;
    readonly approved: number;
    readonly skipped: number;
    readonly failed: number;
}
