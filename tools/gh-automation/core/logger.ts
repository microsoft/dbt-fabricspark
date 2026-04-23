/** Structured logger for gh-automation CLI output. */

export class Logger {
    info(message: string): void {
        console.log(message);
    }

    warn(message: string): void {
        console.warn(`  ⚠ ${message}`);
    }

    error(message: string): void {
        console.error(message);
    }

    dryRun(message: string): void {
        console.log(`[dry-run] ${message}`);
    }

    prHeader(number: number, title: string): void {
        console.log(`\nPR #${number}: ${title}`);
    }

    approving(name: string, runId: number): void {
        console.log(`  → Approving: ${name} (run ${runId})`);
    }

    allClear(): void {
        console.log('  ✓ No workflows awaiting approval.');
    }

    summary(approved: number): void {
        console.log(`\nDone. Approved ${approved} workflow run(s).`);
    }

    watchStatus(intervalSeconds: number): void {
        console.log(`\n⏳ Watching — next reconcile in ${intervalSeconds}s (Ctrl+C to stop)\n`);
    }

    reconcileHeader(): void {
        const now = new Date().toLocaleTimeString();
        console.log(`\n── reconcile @ ${now} ${'─'.repeat(50)}\n`);
    }

    behindMain(branch: string, behindBy: number): void {
        console.log(`  ⬇ Branch ${branch} is ${behindBy} commit(s) behind main`);
    }

    upToDate(branch: string): void {
        console.log(`  ✓ Branch ${branch} is up to date with main.`);
    }

    commentPosted(prNumber: number): void {
        console.log(`  💬 Posted sync comment on PR #${prNumber}`);
    }

    commentFailed(prNumber: number): void {
        console.log(`  ✗ Failed to post comment on PR #${prNumber}`);
    }

    syncSummary(commented: number, upToDate: number, failed: number): void {
        console.log(`\nDone. Commented on ${commented} PR(s), ${upToDate} up to date, ${failed} failed.`);
    }
}
