import { spawnSync } from 'child_process';
import { Command, OptionValues } from 'commander';
import { writeFileSync, readFileSync } from 'fs';
import yaml from 'js-yaml';
import { registry, name, devcontainerFile, pipelineFile } from './const'

const program: Command = new Command()
program
    .name('push-devcontainer')
    .description('Pushes the devcontainer image and updates the devcontainer.json file with the new tag')
    .option('-r, --registry <registry>', 'The container registry to push the image to', registry)
    .option('-n, --name <name>', 'The name of the image to create', name)
    .option('-f, --file <file>', 'The devcontainer.json file to update', devcontainerFile)
    .option('-p, --pipeline <pipeline>', 'The pipeline config file to update', pipelineFile)

const opts: OptionValues = program.parse(process.argv).opts()

const fullImageName = `${opts.registry}/${opts.name}`
const imageTag = readFileSync('.devcontainer/.devcontainer-hash.txt', 'utf8');

console.log(`Updating devcontainer config file: ${opts.file}`);
updateDevcontainerConfigFile(fullImageName, imageTag, opts.file);

console.log(`Updating pipeline config file: ${opts.pipeline}`);
updatePipelineConfigFile(fullImageName, imageTag, opts.pipeline);

if (checkIfImageExists(fullImageName, imageTag)) {
    console.log(`Image ${fullImageName}:${imageTag} already exists in ACR. Skipping push...`);
    process.exit(0);
}

console.log(`Pushing devcontainer image: ${fullImageName}:${imageTag}`);
pushDockerImage(fullImageName, imageTag);

console.log('Done!');

/**
 * Checks if the specified image exists in the registry
 * @param imageName The name of the image to push
 * @param imageTag The tag of the image to push
 * @returns True if the image exists in the registry, false otherwise
 */
function checkIfImageExists(imageName: string, imageTag: string): boolean {
    const cmd = ['docker', 'manifest', 'inspect', `${imageName}:${imageTag}`]

    // Check if manifest exists
    const output = spawnSync(cmd.join(' '), { shell: true });
    if (output.status !== 0) {
        if (output.stderr.toString().includes('no such manifest')) {
            return false;
        }
        throw new Error(`Error checking if image ${imageName}:${imageTag} exists: ${output.stderr.toString()}}`);
    }

    console.log(output.stdout.toString());
    return true;
}

/**
 * Pushes a docker image to the specified registry
 * @param imageName The name of the image to push
 * @param imageTag The tag of the image to push
 */
function pushDockerImage(imageName: string, imageTag: string) {
    const cmd = ['docker', 'image', 'push', `${imageName}:${imageTag}`]
    console.log(`Pushing docker image: ${imageName}:${imageTag}`);
    const output = spawnSync(cmd.join(' '), { stdio: 'inherit', shell: true });
    if (output.status !== 0) {
        throw new Error(`Failed to push docker image: ${imageName}:${imageTag}`);
    }

    console.log(`Successfully pushed docker image: ${imageName}:${imageTag}`);
}

/**
 * Creates/updates a devcontainer.json settings file with the
 * devcontainer image name and tag
 * @param imageName The name of the image
 * @param imageTag The tag of the image
 * @param filename The filename to write/update
 */
function updateDevcontainerConfigFile(imageName: string, imageTag: string, filename: string) {
    const data = {
        "name": "devcontainer",
        "image": `${imageName}:${imageTag}`,
        "remoteUser": "vscode",
        "containerUser": "vscode",
        "runArgs": [
            "--cap-add=SYS_ADMIN",
            "--device=/dev/fuse",
            "--security-opt=apparmor:unconfined",
            "--add-host=host.docker.internal:host-gateway",
            "--pids-limit=-1"
            ],
        "mounts": [
            "type=bind,source=/dev/fuse,target=/dev/fuse",
            "type=bind,source=${localEnv:HOME}/.azure,target=/home/vscode/.azure"
        ],
        "customizations": {
            "vscode": {
                "settings": {
                    "terminal.integrated.gpuAcceleration": "off",
                    "terminal.integrated.enablePersistentSessions": false,
                    "terminal.integrated.scrollback": 5000,
                    "remote.autoForwardPorts": false
                }
            }
        }
    }

    writeFileSync(filename, JSON.stringify(data, null, 4));
}

/**
 * Creates/updates an Azure Pipelines templates file with the
 * devcontainer image name and tag as variables
 * @param imageName The name of the image
 * @param imageTag The tag of the image
 * @param filename The filename to write/update
 */
function updatePipelineConfigFile(imageName: string, imageTag: string, filename: string) {
    const data = {
        variables: {
            devcontainerImageName: imageName,
            devcontainerImageTag: imageTag
        }
    }

    const yamlContent = yaml.dump(data);
    writeFileSync(filename, yamlContent);
}

/**
 * Updates the docker-compose.test.yml file with the new image
 * @param imageName The name of the image
 * @param imageTag The tag of the image
 */
function updateDockerComposeTestFile(imageName: string, imageTag: string) {
    const composeFile = '.devcontainer/docker-compose.test.yml';
    let content = readFileSync(composeFile, 'utf8');
    
    content = content.replace(
        /image: \${DEVCONTAINER_IMAGE}/,
        `image: ${imageName}:${imageTag}`
    );
    
    writeFileSync(composeFile, content);
}