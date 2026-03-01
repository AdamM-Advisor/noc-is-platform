#!/bin/bash
set -e

echo "=== Building frontend ==="
cd /home/runner/workspace/frontend
npm run build

echo "=== Locating real CPython binary ==="
cd /home/runner/workspace

WRAPPER_BIN=$(readlink -f "$(which python3)")
REAL_PYTHON=$(strings "$WRAPPER_BIN" 2>/dev/null | grep -m1 '/nix/store/.*/bin/python$')
LD_LIBS=$(strings "$WRAPPER_BIN" 2>/dev/null | grep -m1 '/nix/store/.*cpplibs/lib')

if [ -z "$REAL_PYTHON" ] || [ ! -x "$REAL_PYTHON" ]; then
    echo "ERROR: Could not find real CPython binary"
    exit 1
fi

echo "Real CPython: $REAL_PYTHON"
$REAL_PYTHON --version

SITE_PACKAGES="/home/runner/workspace/.pythonlibs/lib/python3.11/site-packages"

cat > /home/runner/workspace/run_server.sh << EOF
#!/bin/bash
export PYTHONPATH="$SITE_PACKAGES:\$PYTHONPATH"
export LD_LIBRARY_PATH="$LD_LIBS:\$LD_LIBRARY_PATH"
exec $REAL_PYTHON -m uvicorn backend.main:app --host 0.0.0.0 --port 5000
EOF
chmod +x /home/runner/workspace/run_server.sh

echo "=== Verifying ==="
cat /home/runner/workspace/run_server.sh
export PYTHONPATH="$SITE_PACKAGES"
export LD_LIBRARY_PATH="$LD_LIBS"
$REAL_PYTHON -c "import uvicorn; import duckdb; import fastapi; print('All imports OK')"

echo "=== Cleaning up for deployment ==="
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
find . -name "*.pyc" -delete
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

mkdir -p data uploads temp_chunks exports

echo "=== Build complete ==="
