#!/bin/bash
export PYTHONPATH="/home/runner/workspace/.pythonlibs/lib/python3.11/site-packages:$PYTHONPATH"
export PATH="/home/runner/workspace/.pythonlibs/bin:$PATH"
exec "/nix/store/flbj8bq2vznkcwss7sm0ky8rd0k6kar7-python-wrapped-0.1.0/bin/.python-wrapped" -m uvicorn backend.main:app --host 0.0.0.0 --port 5000
