# ARQUIVOS ROOT - COPIE E COLE

## 1️⃣ .gitignore

Crie arquivo: `.gitignore`

```
# Dependencies
node_modules/
package-lock.json
yarn.lock

# Build
dist/
build/
*.tsbuildinfo

# Logs
logs/
*.log
npm-debug.log*
yarn-debug.log*
yarn-error.log*

# Environment
.env
.env.local
.env.*.local

# IDE
.vscode/
.idea/
*.swp
*.swo
*~
.DS_Store

# Database
*.db
*.sqlite
.postgres/

# Docker
docker-compose.override.yml
.docker/
```

---

## 2️⃣ tsconfig.json

Crie arquivo: `tsconfig.json`

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "commonjs",
    "lib": ["ES2020"],
    "outDir": "./dist",
    "rootDir": "./src",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true,
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "noImplicitAny": true,
    "strictNullChecks": true,
    "strictFunctionTypes": true,
    "noImplicitThis": true,
    "alwaysStrict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noImplicitReturns": true,
    "noFallthroughCasesInSwitch": true,
    "moduleResolution": "node",
    "allowSyntheticDefaultImports": true,
    "paths": {
      "@/*": ["src/*"]
    }
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist", "tests"]
}
```

---

## 3️⃣ jest.config.js

Crie arquivo: `jest.config.js`

```javascript
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/tests'],
  testMatch: ['**/__tests__/**/*.ts', '**/?(*.)+(spec|test).ts'],
  moduleFileExtensions: ['ts', 'js', 'json'],
  collectCoverageFrom: [
    'src/**/*.ts',
    '!src/**/*.d.ts',
  ],
  coveragePathIgnorePatterns: [
    '/node_modules/',
  ],
};
```

---

## 4️⃣ .dockerignore

Crie arquivo: `.dockerignore`

```
node_modules
npm-debug.log
dist
.git
.gitignore
.env
.env.*.local
docker-compose.override.yml
tests
docs
```

---

## 5️⃣ LICENSE

Crie arquivo: `LICENSE`

```
MIT License

Copyright (c) 2026 Giovani Rodrigues

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
```

---

## 6️⃣ .env.example

Crie arquivo: `.env.example`

```
NODE_ENV=development
PORT=3000
LOG_LEVEL=info

# Kafka
KAFKA_BROKER=localhost:9092

# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=event_driven
DB_USER=postgres
DB_PASSWORD=postgres
```

---

## 7️⃣ package.json (O MAIS IMPORTANTE!)

Crie arquivo: `package.json`

```json
{
  "name": "event-driven-kafka-architecture",
  "version": "1.0.0",
  "description": "Production-grade event-driven microservices architecture using Kafka, Node.js, and PostgreSQL. Demonstrates 50% latency optimization and real-world patterns.",
  "main": "dist/index.js",
  "types": "dist/index.d.ts",
  "scripts": {
    "dev": "ts-node src/index.ts",
    "build": "tsc",
    "start": "node dist/index.js",
    "test:unit": "jest --testPathPattern=unit",
    "test:integration": "jest --testPathPattern=integration",
    "test:benchmark": "jest --testPathPattern=benchmark",
    "test:all": "jest",
    "test:watch": "jest --watch",
    "db:migrate": "node scripts/migrate.js",
    "docker:up": "docker-compose up -d",
    "docker:down": "docker-compose down",
    "docker:logs": "docker-compose logs -f",
    "docker:rebuild": "docker-compose down && docker build -f docker/Dockerfile -t event-driven-api:latest . && docker-compose up -d",
    "lint": "eslint src --ext .ts",
    "format": "prettier --write \"src/**/*.ts\"",
    "prebuild": "npm run lint",
    "prestart": "npm run build"
  },
  "keywords": [
    "kafka",
    "event-driven",
    "microservices",
    "nodejs",
    "typescript",
    "postgresql",
    "performance",
    "architecture"
  ],
  "author": "Giovani Rodrigues <giovani@example.com> (https://github.com/GiovaniRodrigo)",
  "license": "MIT",
  "engines": {
    "node": ">=18.0.0",
    "npm": ">=9.0.0"
  },
  "dependencies": {
    "express": "^4.18.2",
    "kafkajs": "^2.2.4",
    "pg": "^8.11.1",
    "uuid": "^9.0.0",
    "dotenv": "^16.3.1",
    "pino": "^8.16.1"
  },
  "devDependencies": {
    "@types/express": "^4.17.17",
    "@types/jest": "^29.5.3",
    "@types/node": "^20.3.1",
    "@typescript-eslint/eslint-plugin": "^6.0.0",
    "@typescript-eslint/parser": "^6.0.0",
    "eslint": "^8.44.0",
    "jest": "^29.5.0",
    "prettier": "^3.0.0",
    "ts-jest": "^29.1.0",
    "ts-node": "^10.9.1",
    "typescript": "^5.1.3"
  }
}
```

---

## ✅ Checklist

- [ ] `.gitignore` criado
- [ ] `tsconfig.json` criado
- [ ] `jest.config.js` criado
- [ ] `.dockerignore` criado
- [ ] `LICENSE` criado
- [ ] `.env.example` criado
- [ ] `package.json` criado

**7 arquivos root = PRONTO!**

Próximo: **04_ARQUIVOS_SRC.md** (para copiar os .ts files)
