import { spawnSync } from 'child_process';
import { Command, OptionValues } from 'commander';
import { readFileSync } from 'fs';
import { registry, name, devcontainerHashFile } from './const'

const program: Command = new Command()
program
    .name('build-devcontainer')
    .description('Builds the devcontainer image')
    .option('-r, --registry <registry>', 'The container registry to push the image to', registry)
    .option('-n, --name <name>', 'The name of the image to create', name)
    .option('-f, --force', 'Force the build of the devcontainer image')

const opts: OptionValues = program.parse(process.argv).opts()

const fullImageName = `${opts.registry}/${opts.name}`

const imageTag = readDevcontainerHash();
if (!opts.force && doesImageTagExist(fullImageName, imageTag)) {
    console.log(`Image ${fullImageName}:${imageTag} already exists. Skipping build...`);
    process.exit(0);
};

console.log('Building devcontainer image...')
buildDevcontainerImage(fullImageName, imageTag)

/**
 * Reads the file hash of the devcontainer contents from the .devcontainer/.devcontainer-hash.txt file
 * @returns The hash of the devcontainer contents
 */
function readDevcontainerHash() {
    const filename = devcontainerHashFile;
    const tag = readFileSync(filename, 'utf8');
    return tag;
}

/**
 * Builds the devcontainer image using the devcontainer cli
 * @param imageName The name of the image to create
 * @param imageTag The tag of the image to create
 */
function buildDevcontainerImage(imageName: string, imageTag: string) {
    const cmd = ['devcontainer', 'build', '--workspace-folder', '.', '--config',
        '.devcontainer/devcontainer.local.json', '--image-name', `${imageName}:${imageTag}`]
    const output = spawnSync(cmd.join(' '), { stdio: 'inherit', shell: true });
}

/**
 * Checks if an image tag exists in the local docker registry
 * @param imageName Name of the image
 * @param imageTag Tag of the image
 * @returns True if the image already exists, false otherwise
 */
function doesImageTagExist(imageName: string, imageTag: string): boolean {
    try {
        const output = spawnSync(`docker images ${imageName}:${imageTag}`, { shell: true });
        console.log(output.stdout.toString());
        return output.stdout.toString().split('\n').length > 2;
    } catch (error) {
        console.error(error);
        return false;
    }
}