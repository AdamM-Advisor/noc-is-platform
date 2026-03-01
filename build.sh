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
rm -rf frontend/index.html
rm -rf frontend/package.json
rm -rf frontend/package-lock.json
rm -rf frontend/vite.config.js
rm -rf node_modules
rm -rf .cache
rm -rf .local
rm -rf .upm
rm -rf attached_assets
rm -rf temp_chunks
rm -rf exports
rm -rf uv.lock
rm -rf package.json
rm -rf package-lock.json
find . -name "*.pyc" -delete
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

mkdir -p data uploads temp_chunks exports

echo "=== Checking Python ==="
PYTHON_BIN=""
if command -v python3 &> /dev/null; then
    PYTHON_BIN=$(command -v python3)
elif [ -x "$HOME/workspace/.pythonlibs/bin/python3" ]; then
    PYTHON_BIN="$HOME/workspace/.pythonlibs/bin/python3"
fi

if [ -n "$PYTHON_BIN" ]; then
    echo "Python found at: $PYTHON_BIN"
    $PYTHON_BIN --version
    $PYTHON_BIN -c "import uvicorn; print('uvicorn available')"
else
    echo "ERROR: Python not found!"
    exit 1
fi

echo "=== Build complete ==="
