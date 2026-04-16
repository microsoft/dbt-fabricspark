import { createHash } from 'crypto';
import * as path from 'path';
import { spawnSync } from 'child_process';
import { readFileSync, writeFileSync } from 'fs';
import { Command, OptionValues } from 'commander';
import { devcontainerHashFile } from './const';

const program: Command = new Command()
program
    .name('compute-devcontainer-hash')
    .description('Computes the file hash of the .devcontainer folder and outputs to a file')
    .option('-o, --output <output>', 'The output file to write the hash to', devcontainerHashFile)

const opts: OptionValues = program.parse(process.argv).opts()

writeDevcontainerHashToFile(opts.output)

/**
 * Computes the file hash of the devcontainer directory and writes it to a file
 * @returns The file hash
 */
export function writeDevcontainerHashToFile(filename: string) {
    const hash = computeHash('.devcontainer');

    writeFileSync(filename, hash)
    console.log(`devcontainer hash: ${hash}`);

    return hash
}

/**
 * Computes a file hash for the given directory (recursively)
 * @param directory The directory to compute the file hash for
 * @returns The file hash
 */
function computeHash(directory: string): string {
    const hash = createHash('sha256');
    // git ls-files will automatically exclude anything gitignored
    const files = spawnSync('git', ['ls-files', directory]).stdout
        .toString()
        .split('\n')
        .map(f => f.trim())
        .filter(f => f !== '')
        .sort()

    const disallowedFiles = [
        '.devcontainer-hash.txt',
        '.env',
        '.env.local',
        'project.json',
        'devcontainer.json',
        'README.md'
    ];

    for (const filePath of files) {
        const file = path.basename(filePath)
        // Skip adding disallowed files to the hash
        if (disallowedFiles.includes(file)) {
            continue;
        }

        console.log(`file: ${filePath}`);
        // Convert CRLF to LF to avoid windows -> linux line ending issues
        const data = readFileSync(filePath, 'utf8').replace(/\r\n/g, '\n');
        hash.update(data)

        // Calculate the hash of each file
        // to show diffs - helps debugging
        // untracked file failures from CRLF -> LF
        const tempHash = createHash("sha256");
        tempHash.update(data);
        const fileHash = tempHash.digest("hex");
        console.log(`hash: ${fileHash}`);
    }

    return hash.digest('hex');
}