/**
 * Service for querying GitHub pull requests.
 *
 * Depends on GhClient (Dependency Inversion) — never calls `gh` directly.
 */

import { GhClient } from './gh-client.js';
import type { PullRequest, PullRequestWithAuthor } from './types.js';

const COPILOT_AUTHOR = 'copilot-swe-agent';
const DEPENDABOT_AUTHOR = 'app/dependabot';

export class PullRequestService {
    constructor(private readonly client: GhClient) {}

    /** List all open PRs authored by the Copilot agent. */
    listCopilotPRs(): PullRequest[] | null {
        const allPRs = this.client.execJson<PullRequestWithAuthor[]>([
            'pr', 'list',
            '--repo', this.client.repoSlug,
            '--state', 'open',
            '--json', 'number,title,url,headRefName,author',
        ]);

        if (!allPRs) return null;
        return allPRs.filter((pr) => pr.author.login === COPILOT_AUTHOR);
    }

    /** List all open PRs authored by Dependabot. */
    listDependabotPRs(): PullRequest[] | null {
        return this.client.execJson<PullRequest[]>([
            'pr', 'list',
            '--repo', this.client.repoSlug,
            '--state', 'open',
            '--author', DEPENDABOT_AUTHOR,
            '--json', 'number,title,url,headRefName',
        ]);
    }

    /** Fetch a single PR by number. */
    getByNumber(prNumber: string): PullRequest | null {
        return this.client.execJson<PullRequest>([
            'pr', 'view', prNumber,
            '--repo', this.client.repoSlug,
            '--json', 'number,title,url,headRefName',
        ]);
    }
}
