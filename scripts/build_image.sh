#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

docker rmi sonic_lw:latest 2>/dev/null || true
sleep 1
docker build -t sonic_lw:latest -f "$PROJECT_DIR/docker/Dockerfile" "$PROJECT_DIR"
