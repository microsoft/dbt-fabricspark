/**
 * Runs an action once, then in an infinite reconcile loop if watch mode is on.
 *
 * Open/Closed: accepts any action callback — new behaviors can be plugged in
 * without modifying WatchRunner itself.
 */

import { setTimeout as sleep } from 'node:timers/promises';
import { Logger } from './logger.js';

const DEFAULT_SLEEP_SECONDS = 60;

export class WatchRunner {
    private readonly intervalMs: number;

    constructor(
        private readonly watch: boolean,
        private readonly logger: Logger,
        sleepSeconds: number = DEFAULT_SLEEP_SECONDS,
    ) {
        this.intervalMs = sleepSeconds * 1000;
    }

    /** Execute `action` once, then loop if watch mode is enabled. */
    async run(action: () => void | Promise<void>): Promise<void> {
        await action();

        if (!this.watch) return;

        const intervalSec = this.intervalMs / 1000;
        this.logger.watchStatus(intervalSec);

        while (true) {
            await sleep(this.intervalMs);
            this.logger.reconcileHeader();
            await action();
            this.logger.watchStatus(intervalSec);
        }
    }
}
