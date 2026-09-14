# syntax=docker/dockerfile:1
# Multi-stage build for the pnpm workspace. The build stage installs the full
# workspace, compiles the contracts package and the backend, then uses
# `pnpm deploy` to emit a self-contained, production-only backend (its dist plus
# a node_modules that already has the built @kafka-demo/contracts injected). The
# runtime stage ships only that.

# ---- Build stage ----
FROM node:20-alpine AS build
RUN corepack enable
WORKDIR /repo

# Install deps against the lockfile using only the manifests (better caching).
COPY pnpm-workspace.yaml package.json pnpm-lock.yaml ./
COPY packages/contracts/package.json packages/contracts/package.json
COPY packages/backend/package.json packages/backend/package.json
RUN pnpm install --frozen-lockfile

# Bring in the sources and build contracts before the backend (backend imports it).
COPY packages/contracts packages/contracts
COPY packages/backend packages/backend
RUN pnpm --filter @kafka-demo/contracts build \
 && pnpm --filter @kafka-demo/backend build

# Produce a self-contained deployment of the backend with prod deps only.
RUN pnpm --filter @kafka-demo/backend --prod deploy /app

# ---- Runtime stage ----
FROM node:20-alpine
WORKDIR /app
ENV NODE_ENV=production

COPY --from=build /app ./

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=5 \
  CMD node -e "require('http').get('http://localhost:3000/health',(r)=>{process.exit(r.statusCode===200?0:1)}).on('error',()=>process.exit(1))"

CMD ["node", "dist/index.js"]
