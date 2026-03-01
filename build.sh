#!/bin/bash
set -e

echo "=== Building frontend ==="
cd /home/runner/workspace/frontend
npm run build

echo "=== Locating Python ==="
cd /home/runner/workspace

PYTHON_REAL=$(readlink -f "$(which python3)")
echo "Python binary: $PYTHON_REAL"
$PYTHON_REAL --version
$PYTHON_REAL -c "import uvicorn; print('uvicorn OK')"

PYTHONLIBS_DIR="/home/runner/workspace/.pythonlibs"
SITE_PACKAGES="$PYTHONLIBS_DIR/lib/python3.11/site-packages"

cat > /home/runner/workspace/run_server.sh << EOF
#!/bin/bash
export PYTHONPATH="$SITE_PACKAGES:\$PYTHONPATH"
export PATH="$PYTHONLIBS_DIR/bin:\$PATH"
exec "$PYTHON_REAL" -m uvicorn backend.main:app --host 0.0.0.0 --port 5000
EOF
chmod +x /home/runner/workspace/run_server.sh

echo "=== Testing run script ==="
cat /home/runner/workspace/run_server.sh

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
