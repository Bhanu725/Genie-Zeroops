#!/usr/bin/env bash
# Shared settings. Override with env vars, e.g. PROFILE=other ./scripts/status.sh
PROFILE="${PROFILE:-zeroops-poc2}"
TARGET="${TARGET:-dev}"
CATALOG="${CATALOG:-workspace}"
SCHEMA="${SCHEMA:-zeroops_poc}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
