#!/bin/bash
set -e

echo "=== Building frontend ==="
cd /home/runner/workspace/frontend
npm run build

echo "=== Build complete ==="
