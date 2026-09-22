#!/usr/bin/env bash
set -e

# ==============================================================================
# Final Verification Gate — Comprehensive Quality & Distributed Consistency Gate
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

export NODE_ENV="test"
export DATABASE_URL="${DATABASE_URL:-postgresql://postgres:postgres@localhost:5432/event_driven_test}"
export KAFKA_BROKER="${KAFKA_BROKER:-localhost:9092}"
export LOG_LEVEL="warn"

echo "======================================================================"
echo "          DISTRIBUTED SYSTEM VERIFICATION GATE & TEST RUNNER          "
echo "======================================================================"

GATE_TYPESCRIPT="PENDING"
GATE_BUILD="PENDING"
GATE_UNIT="PENDING"
GATE_INTEGRATION="PENDING"
GATE_HTTP="PENDING"
GATE_REALTIME="PENDING"
GATE_E2E="PENDING"
INFRA_STARTED=0

cleanup() {
  if [ "$INFRA_STARTED" -eq 1 ]; then
    echo ""
    echo ">> Tearing down test containers..."
    docker compose -f docker-compose.yml -f docker-compose.dev.yml down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# 1. Check / Bootstrap Test Infrastructure
echo ">> Checking Test Infrastructure..."
if docker ps >/dev/null 2>&1; then
  echo ">> Docker daemon is active. Starting test postgres and kafka..."
  docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d postgres kafka zookeeper
  INFRA_STARTED=1
  
  echo ">> Waiting for PostgreSQL readiness..."
  if ! timeout 60 bash -c 'until docker exec postgres pg_isready -U postgres -d event_driven_test >/dev/null 2>&1; do sleep 1; done'; then
    echo "PostgreSQL failed to become ready within 60 seconds" >&2
    exit 1
  fi
  echo ">> PostgreSQL is ready."

  echo ">> Waiting for Kafka broker readiness..."
  if ! timeout 60 bash -c 'until nc -z localhost 9092; do sleep 1; done' 2>/dev/null; then
    echo "Kafka failed to become ready within 60 seconds" >&2
    exit 1
  fi
  echo ">> Kafka is ready."
else
  echo ">> Docker daemon not directly accessible. Checking if localhost services are reachable..."
  if ! timeout 5 bash -c 'until nc -z localhost 5432; do sleep 1; done' 2>/dev/null; then
    echo "ERROR: PostgreSQL is not reachable at localhost:5432" >&2
    exit 1
  fi
  if ! timeout 5 bash -c 'until nc -z localhost 9092; do sleep 1; done' 2>/dev/null; then
    echo "ERROR: Kafka broker is not reachable at localhost:9092" >&2
    exit 1
  fi
  echo ">> PostgreSQL and Kafka are reachable on localhost."
fi

# 2. Initialize Database Schema
echo ">> Initializing Database Schema on ${DATABASE_URL}..."
if ! node -e "
  const { Pool } = require('pg');
  const fs = require('fs');
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  const sql = fs.readFileSync('scripts/init-db.sql', 'utf8');
  pool.query(sql)
    .then(() => {
      console.log('>> Database schema initialized successfully.');
      process.exit(0);
    })
    .catch((err) => {
      console.error('>> Database schema initialization failed:', err.message);
      process.exit(1);
    });
"; then
  echo "Failed to initialize database schema!" >&2
  exit 1
fi

# 3. TypeScript Typecheck
echo ""
echo ">> 1. Running TypeScript Typecheck..."
if npx tsc --noEmit; then
  GATE_TYPESCRIPT="PASS"
else
  GATE_TYPESCRIPT="FAIL"
  echo "TypeScript typecheck failed!"
  exit 1
fi

# 4. TypeScript Build
echo ""
echo ">> 2. Running TypeScript Build..."
if npm run build; then
  GATE_BUILD="PASS"
else
  GATE_BUILD="FAIL"
  echo "Build failed!"
  exit 1
fi

# 5. Unit Tests
echo ""
echo ">> 3. Running Unit Tests..."
if npm run test:unit; then
  GATE_UNIT="PASS"
else
  GATE_UNIT="FAIL"
  echo "Unit tests failed!"
  exit 1
fi

# 6. HTTP API Tests
echo ""
echo ">> 4. Running HTTP API Tests..."
if npm run test:http; then
  GATE_HTTP="PASS"
else
  GATE_HTTP="FAIL"
  echo "HTTP tests failed!"
  exit 1
fi

# 7. Realtime Gateway & Consumer Tests
echo ""
echo ">> 5. Running Realtime Tests..."
if npm run test:realtime; then
  GATE_REALTIME="PASS"
else
  GATE_REALTIME="FAIL"
  echo "Realtime tests failed!"
  exit 1
fi

# 8. End-to-End Fulfillment Tests
echo ""
echo ">> 6. Running E2E Tests..."
if npm run test:e2e; then
  GATE_E2E="PASS"
else
  GATE_E2E="FAIL"
  echo "E2E tests failed!"
  exit 1
fi

# 9. Integration Tests (Requires Real PostgreSQL & Kafka)
echo ""
echo ">> 7. Running PostgreSQL & Kafka Integration Tests..."
if npm run test:integration; then
  GATE_INTEGRATION="PASS"
else
  GATE_INTEGRATION="FAIL"
  echo "Integration tests failed (Required infrastructure unavailable or assertions failed)!"
  exit 1
fi

echo ""
echo "======================================================================"
echo "                   FINAL QUALITY GATE SUMMARY                         "
echo "======================================================================"
printf "%-35s %s\n" "TypeScript Typecheck" "$GATE_TYPESCRIPT"
printf "%-35s %s\n" "Application Build" "$GATE_BUILD"
printf "%-35s %s\n" "Unit Tests" "$GATE_UNIT"
printf "%-35s %s\n" "HTTP API Tests" "$GATE_HTTP"
printf "%-35s %s\n" "Realtime Tests" "$GATE_REALTIME"
printf "%-35s %s\n" "End-to-End Tests" "$GATE_E2E"
printf "%-35s %s\n" "PostgreSQL & Kafka Integration" "$GATE_INTEGRATION"
echo "======================================================================"
echo "ALL GATES GREEN."
