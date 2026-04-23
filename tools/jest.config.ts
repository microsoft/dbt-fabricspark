export default {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/gh-automation'],
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
    'gh-automation/**/*.ts',
    '!gh-automation/**/*.test.ts',
    '!gh-automation/**/*.spec.ts',
  ],
};
