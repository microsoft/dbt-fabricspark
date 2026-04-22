/**
 * This script is a utility that exits with a non-zero code if there
 * are any untracked files or unstaged changes in the workspace.
 *
 * This is used in GCI to guard against inconsistencies between checked-in
 * and generated files.
 */

import { spawnSync } from 'child_process';

const task = spawnSync(
    'git ls-files --other --modified --directory --exclude-standard --no-empty-directory',
    { shell: true }
);

if (task.status != 0) {
    console.error('Error running git ls-files');
    process.exit(1);
}

if (task.stdout.toString().trim() != '') {
    console.error('There are untracked files or unstaged changes in the workspace:');
    console.error(task.stdout.toString());

    console.info('git diff:');
    const task1 = spawnSync('git diff', { shell: true });
    console.info(task1.stdout.toString());

    process.exit(1);
}