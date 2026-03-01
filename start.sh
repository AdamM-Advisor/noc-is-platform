#!/bin/bash

PYTHON_BIN=""

if command -v python3 &> /dev/null; then
    PYTHON_BIN="python3"
elif [ -x "$HOME/workspace/.pythonlibs/bin/python3" ]; then
    PYTHON_BIN="$HOME/workspace/.pythonlibs/bin/python3"
elif [ -x "/home/runner/workspace/.pythonlibs/bin/python3" ]; then
    PYTHON_BIN="/home/runner/workspace/.pythonlibs/bin/python3"
else
    for p in /nix/store/*/bin/python3; do
        if [ -x "$p" ]; then
            PYTHON_BIN="$p"
            break
        fi
    done
fi

if [ -z "$PYTHON_BIN" ]; then
    echo "ERROR: Python3 not found"
    exit 1
fi

echo "Using Python: $PYTHON_BIN"
$PYTHON_BIN --version

exec $PYTHON_BIN -m uvicorn backend.main:app --host 0.0.0.0 --port 5000
