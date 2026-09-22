# ==============================================================================
# Multi-Stage Dockerfile for Event-Driven Kafka Architecture
# Target stages:
#   - base: Common runtime base image with system utilities & security tools
#   - development: Full dev dependencies, source code & live reload (ts-node)
#   - builder: TypeScript compilation stage to generate /app/dist
#   - production: Minimal hardened image, non-root user, compiled JS only
# ==============================================================================

# ---- Stage 1: Base Runtime Image ----
FROM node:20-alpine AS base

# Install dumb-init for proper PID 1 signal forwarding & zombie process reaping
RUN apk add --no-cache dumb-init curl

WORKDIR /app

# ---- Stage 2: Development Environment ----
FROM base AS development

ENV NODE_ENV=development

# Install all dependencies (including devDependencies for ts-node, typescript, etc.)
COPY package.json package-lock.json ./
RUN npm ci

# Copy configuration and source files
COPY tsconfig.json ./
COPY src ./src

EXPOSE 3000

# Run in development mode with live execution
CMD ["npm", "run", "dev"]

# ---- Stage 3: Builder (Compile TypeScript) ----
FROM base AS builder

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src

RUN npm run build

# ---- Stage 4: Production Runtime ----
FROM base AS production

ENV NODE_ENV=production

# Install only production dependencies and clean npm cache
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

# Copy compiled JavaScript from builder stage
COPY --from=builder --chown=node:node /app/dist ./dist

# Switch to non-privileged user for security hardening
USER node

EXPOSE 3000

# Production container healthcheck
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health',(r)=>{process.exit(r.statusCode===200?0:1)}).on('error',()=>process.exit(1))"

# Use dumb-init as entrypoint for graceful shutdown handling
ENTRYPOINT ["dumb-init", "--"]
CMD ["node", "dist/index.js"]
