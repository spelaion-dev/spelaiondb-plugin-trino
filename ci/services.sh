#!/usr/bin/env bash
# Starts what the tests need and waits until it answers: CI runs this before the tests
# (the shared workflow looks for it), and so can you.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f testing/compose.yaml up -d --wait
