#!/usr/bin/env bash
# Shared settings. Override with env vars, e.g. PROFILE=other ./scripts/status.sh
PROFILE="${PROFILE:-zeroops-poc3}"
TARGET="${TARGET:-dev}"
CATALOG="${CATALOG:-workspace}"
SCHEMA="${SCHEMA:-zeroops_poc}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: stop /api/... being rewritten as a path
cd "$ROOT"
