#!/usr/bin/env bash
# Wipe the mongo volume and re-provision the root user.
set -euo pipefail

cd "$(dirname "$0")"

docker compose down -v
docker compose up -d
