import { spawnSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { basename, dirname, join, posix, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const projectDirectory = resolve(scriptDirectory, '../..');
const envFile = join(projectDirectory, 'test.env');
const distDirectory = join(projectDirectory, 'dist');

const envNames = {
    accountName: 'PUBLISH_BLOB_ACCOUNT_NAME',
    containerName: 'PUBLISH_BLOB_CONTAINER_NAME',
    pathPrefix: 'PUBLISH_BLOB_PATH_PREFIX',
    publicBaseUrl: 'PUBLISH_BLOB_PUBLIC_BASE_URL',
} as const;

function parseEnvValue(rawValue: string, lineNumber: number): string {
    const value = rawValue.trim();
    const quote = value[0];

    if (quote === '"' || quote === "'") {
        if (value.at(-1) !== quote) {
            throw new Error(`Invalid quoted value in test.env on line ${lineNumber}.`);
        }

        const unquoted = value.slice(1, -1);
        if (quote === "'") {
            return unquoted;
        }

        return unquoted
            .replaceAll('\\n', '\n')
            .replaceAll('\\r', '\r')
            .replaceAll('\\t', '\t')
            .replaceAll('\\"', '"')
            .replaceAll('\\\\', '\\');
    }

    return value.replace(/\s+#.*$/, '').trim();
}

function loadTestEnv(): void {
    if (!existsSync(envFile)) {
        throw new Error('test.env was not found at the repository root.');
    }

    for (const [index, rawLine] of readFileSync(envFile, 'utf8').split(/\r?\n/).entries()) {
        const line = rawLine.trim();
        if (line === '' || line.startsWith('#')) {
            continue;
        }

        const match = line.match(/^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$/);
        if (!match) {
            throw new Error(`Invalid entry in test.env on line ${index + 1}.`);
        }

        const [, name, rawValue] = match;
        if (process.env[name] === undefined) {
            process.env[name] = parseEnvValue(rawValue, index + 1);
        }
    }
}

function requireEnv(name: string): string {
    const value = process.env[name]?.trim();
    if (!value) {
        throw new Error(`Environment variable ${name} must be set.`);
    }

    return value;
}

function assertAzureLogin(): void {
    const result = spawnSync('az', ['account', 'show', '--output', 'none'], {
        stdio: 'ignore',
    });

    if (result.error && 'code' in result.error && result.error.code === 'ENOENT') {
        throw new Error('Azure CLI is not installed or is not available on PATH.');
    }
    if (result.error) {
        throw new Error(`Unable to run Azure CLI: ${result.error.message}`);
    }
    if (result.status !== 0) {
        throw new Error("Azure CLI is not authenticated. Run 'az login' first.");
    }
}

function findWheel(): string {
    if (!existsSync(distDirectory)) {
        throw new Error('dist/ does not exist after the build completed.');
    }

    const wheels = readdirSync(distDirectory)
        .filter((entry) => entry.endsWith('.whl'))
        .sort();

    if (wheels.length !== 1) {
        throw new Error(`Expected exactly one wheel in dist/, found ${wheels.length}.`);
    }

    return join(distDirectory, wheels[0]);
}

function uploadWheel(wheelPath: string): void {
    const accountName = requireEnv(envNames.accountName);
    const containerName = requireEnv(envNames.containerName);
    const pathPrefix = requireEnv(envNames.pathPrefix);
    requireEnv(envNames.publicBaseUrl);

    const wheelName = basename(wheelPath);
    const blobName = posix.join(pathPrefix, wheelName);

    console.info(`Uploading ${wheelName} to the configured blob destination...`);
    const result = spawnSync(
        'az',
        [
            'storage',
            'blob',
            'upload',
            '--account-name',
            accountName,
            '--container-name',
            containerName,
            '--name',
            blobName,
            '--file',
            wheelPath,
            '--overwrite',
            '--auth-mode',
            'login',
            '--output',
            'none',
        ],
        { stdio: 'ignore' }
    );

    if (result.error) {
        throw new Error(`Unable to run Azure CLI upload: ${result.error.message}`);
    }
    if (result.status !== 0) {
        throw new Error(`Azure CLI upload failed with exit code ${result.status ?? 'unknown'}.`);
    }

    console.info(`Uploaded ${wheelName}.`);
}

function main(): void {
    loadTestEnv();
    const wheelPath = findWheel();
    assertAzureLogin();
    uploadWheel(wheelPath);
}

try {
    main();
} catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`ERROR: ${message}`);
    process.exitCode = 1;
}
