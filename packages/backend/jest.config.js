module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/tests'],
  testMatch: ['**/__tests__/**/*.ts', '**/?(*.)+(spec|test).ts'],
  moduleFileExtensions: ['ts', 'js', 'json'],
  // Resolve the workspace contracts package straight to its TypeScript source
  // so tests run without a prior `contracts` build step.
  moduleNameMapper: {
    '^@kafka-demo/contracts$': '<rootDir>/../contracts/src/index.ts',
  },
  collectCoverageFrom: ['src/**/*.ts', '!src/**/*.d.ts'],
  coveragePathIgnorePatterns: ['/node_modules/'],
};
