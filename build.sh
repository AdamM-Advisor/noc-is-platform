#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -d "$SCRIPT_DIR/frontend/src" ] && [ -d "$SCRIPT_DIR/frontend/node_modules" ]; then
    echo "=== Building frontend ==="
    cd "$SCRIPT_DIR/frontend"
    npm run build
    echo "=== Build complete ==="
else
    echo "=== Skipping frontend build (pre-built dist used) ==="
fi
