#!/usr/bin/env bash
#
# watchdog.sh — host-memory watchdog for the event-driven stack.
#
# Polls the host's available memory. If it stays below a threshold for several
# consecutive checks (the stack is about to drive the machine into swap and
# freeze it), it gracefully stops the Compose stack and logs why. It measures
# the host itself, so the threshold scales to whatever machine it runs on.
#
# Tunables (env vars):
#   WATCHDOG_INTERVAL   seconds between checks              (default 5)
#   WATCHDOG_BREACHES   consecutive breaches before acting  (default 3)
#   WATCHDOG_THRESHOLD_MB  absolute floor in MB; if unset,  (default: derived)
#                          derived as max(512, 9% of MemTotal)
#
# Usage: scripts/watchdog.sh   (Ctrl-C to stop; start.sh runs it in background)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

INTERVAL="${WATCHDOG_INTERVAL:-5}"
BREACHES="${WATCHDOG_BREACHES:-3}"

# --- resolve the compose command (must match start.sh) -----------------------
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "ERROR: neither 'docker compose' nor 'docker-compose' is available." >&2
  exit 1
fi

# --- derive the threshold from the host (system measures itself) -------------
TOTAL_MB=$(awk '/^MemTotal:/ {printf "%d", $2/1024}' /proc/meminfo)
if [ -n "${WATCHDOG_THRESHOLD_MB:-}" ]; then
  THRESHOLD_MB="$WATCHDOG_THRESHOLD_MB"
else
  THRESHOLD_MB=$(( TOTAL_MB * 9 / 100 ))
  if [ "$THRESHOLD_MB" -lt 512 ]; then THRESHOLD_MB=512; fi
fi

log() { echo "$(date -Is) [watchdog] $*"; }

trap 'log "stopping (signal received)"; exit 0' INT TERM

log "watching host memory: total=${TOTAL_MB}MB threshold=${THRESHOLD_MB}MB interval=${INTERVAL}s breaches=${BREACHES}"

streak=0
while true; do
  # MemAvailable is the kernel's estimate of memory available without swapping.
  AVAIL_MB=$(awk '/^MemAvailable:/ {printf "%d", $2/1024}' /proc/meminfo)

  if [ "$AVAIL_MB" -lt "$THRESHOLD_MB" ]; then
    streak=$(( streak + 1 ))
    log "LOW MEMORY: available=${AVAIL_MB}MB < ${THRESHOLD_MB}MB (breach ${streak}/${BREACHES})"
    if [ "$streak" -ge "$BREACHES" ]; then
      log "threshold sustained — stopping the stack to protect the host"
      "${COMPOSE[@]}" stop || log "WARNING: '${COMPOSE[*]} stop' returned non-zero"
      log "stack stopped. Exiting. Re-run scripts/start.sh to bring it back up."
      exit 0
    fi
  else
    if [ "$streak" -ne 0 ]; then
      log "recovered: available=${AVAIL_MB}MB >= ${THRESHOLD_MB}MB (streak reset)"
    fi
    streak=0
  fi

  sleep "$INTERVAL"
done
