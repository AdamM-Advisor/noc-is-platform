#!/bin/bash
set -e

echo "=== Building frontend ==="
cd /home/runner/workspace/frontend
npm run build

echo "=== Cleaning up for deployment ==="
cd /home/runner/workspace

rm -rf data
rm -rf uploads
rm -rf frontend/node_modules
rm -rf frontend/src
rm -rf frontend/.vite
rm -rf node_modules
rm -rf .cache
rm -rf .local
rm -rf .upm
rm -rf attached_assets
rm -rf temp_chunks
rm -rf exports
find . -name "*.pyc" -delete
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

mkdir -p data uploads temp_chunks exports

echo "=== Build complete ==="
echo "Deployment size:"
du -sh . --exclude=.git --exclude=.pythonlibs 2>/dev/null
