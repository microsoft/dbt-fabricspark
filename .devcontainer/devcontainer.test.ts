import { execSync } from 'child_process';
import { mkdirSync, readFileSync, writeFileSync } from 'fs';
import { join, resolve } from 'path';
import { load } from 'js-yaml';

const workspaceRoot = execSync('git rev-parse --show-toplevel').toString().trim();
const devcontainerJsonPath = join(workspaceRoot, '.devcontainer', 'devcontainer.json');

const devcontainerConfig = JSON.parse(readFileSync(devcontainerJsonPath, 'utf-8'));
const devcontainerImage: string = devcontainerConfig.image;
if (!devcontainerImage) throw new Error('Could not read image from devcontainer.json');

const workflowImageChecks: { file: string; jobKey: string }[] = [
  { file: '.github/workflows/ci.yml',       jobKey: 'linux' },
  { file: '.github/workflows/release.yml',  jobKey: 'release-version' },
];

describe('Devcontainer Tests', () => {
  const timestamp = new Date().toISOString().replace(/[-:]/g, '').replace('T', '_').split('.')[0];
  const logDir = join(workspaceRoot, 'logs', 'test-runs', timestamp);
  let containerId: string;

  beforeAll(() => {
    mkdirSync(logDir, { recursive: true });
    
    const cmd = [
      'docker run -d --name spark-devcontainer-test --cap-add=SYS_ADMIN',
      '--device=/dev/fuse --security-opt=apparmor:unconfined',
      '-v /dev/fuse:/dev/fuse:rw',
      '--user vscode',
      devcontainerImage,
      'sleep infinity'
    ].join(' ');
    
    containerId = execSync(cmd).toString().trim();
    writeFileSync(join(logDir, 'docker-run.log'), containerId);
    
    for (let i = 0; i < 30; i++) {
      const status = execSync('docker inspect spark-devcontainer-test --format="{{.State.Status}}"', { encoding: 'utf8' }).trim();
      if (status === 'running') break;
      execSync('sleep 1');
    }
  });

  afterAll(() => {
    if (containerId) execSync(`docker rm -f spark-devcontainer-test`, { stdio: 'pipe' });
  });

  test('Verify container is running', () => {
    const status = execSync('docker inspect spark-devcontainer-test --format="{{.State.Status}}"', { encoding: 'utf8' }).trim();
    expect(status).toBe('running');
    expect(containerId).toBeTruthy();
  });

  test.each(workflowImageChecks)(
    'devcontainer.json and $file use the same container image',
    ({ file, jobKey }) => {
      const workflow = load(readFileSync(join(workspaceRoot, file), 'utf-8')) as any;
      const image = workflow.jobs[jobKey]?.container?.image;
      expect(image).toBeDefined();
      expect(devcontainerImage).toBe(image);
    },
  );

  test('Verify base image tools', () => {
    const output = execSync(
      'docker exec spark-devcontainer-test bash -c "hatch --version && /opt/spark/bin/spark-submit --version 2>&1"',
      { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }
    );
    writeFileSync(join(logDir, 'base-tools.log'), output);
    expect(output).toContain('Hatch');
    expect(output).toContain('version');
  });

  test('Spark Shell SELECT 1', () => {
    const output = execSync(
      'docker exec spark-devcontainer-test bash -c \'echo "spark.sql(\\"SELECT 1\\").show()" | /opt/spark/bin/spark-shell --master local[1] 2>&1\'',
      { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }
    );
    writeFileSync(join(logDir, 'spark-shell.log'), output);
    expect(output).toContain('|  1|');
  });

  test('Run post-create commands', () => {
    const output = execSync(
      'docker exec spark-devcontainer-test bash -c "/tmp/overlay/post-create-commands.sh 2>&1"',
      { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }
    );
    writeFileSync(join(logDir, 'post-create.log'), output);
    expect(output).toContain('Hatch');
  });

  test('Run post-attach commands', () => {
    const output = execSync(
      'docker exec spark-devcontainer-test bash -c "/tmp/overlay/post-attach-commands.sh 2>&1"',
      { encoding: 'utf8', timeout: 60000, stdio: ['pipe', 'pipe', 'pipe'] }
    );
    writeFileSync(join(logDir, 'post-attach.log'), output);
    expect(output).toContain('SPARK DEVCONTAINER READY');
    expect(output).toContain('Livy Server');
  });

  test('Livy health check', () => {
    let healthy = false;
    for (let i = 0; i < 30; i++) {
      try {
        const output = execSync(
          'docker exec spark-devcontainer-test curl -s http://localhost:8998/sessions',
          { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }
        );
        if (output.includes('sessions')) {
          writeFileSync(join(logDir, 'livy-health.log'), output);
          healthy = true;
          break;
        }
      } catch (e) { }
      execSync('sleep 1', { stdio: 'pipe' });
    }
    expect(healthy).toBe(true);
  });

  test('Verify Livy logs exist', () => {
    const output = execSync(
      'docker exec spark-devcontainer-test bash -c "ls -la /tmp/livy-logs/ && cat /tmp/livy-logs/livy-server.log 2>&1"',
      { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }
    );
    writeFileSync(join(logDir, 'livy-logs.log'), output);
    expect(output).toContain('livy-server.log');
    expect(output.length).toBeGreaterThan(0);
  });
});

