export default {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/.devcontainer'],
  testMatch: ['**/__tests__/**/*.ts', '**/?(*.)+(spec|test).ts'],
  transform: {
    '^.+\\.ts$': ['ts-jest', {
      tsconfig: {
        module: 'commonjs',
        esModuleInterop: true,
        isolatedModules: true,
      },
    }],
  },
  moduleFileExtensions: ['ts', 'js', 'json'],
  collectCoverageFrom: [
    '.devcontainer/**/*.ts',
    '!.devcontainer/**/*.test.ts',
    '!.devcontainer/**/*.spec.ts',
  ],
  testTimeout: 300000, // 5 minutes for container operations
};
