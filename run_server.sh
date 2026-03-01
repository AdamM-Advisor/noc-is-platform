#!/bin/bash
export PYTHONPATH="/home/runner/workspace/.pythonlibs/lib/python3.11/site-packages:$PYTHONPATH"
export LD_LIBRARY_PATH="/nix/store/pya3p1ihjm446jpqpql93542cirqyn23-cpplibs/lib:/nix/store/c2qsgf2832zi4n29gfkqgkjpvmbmxam6-zlib-1.3.1/lib:/nix/store/f7rcazhd826xlcz43il4vafv28888cgj-glib-2.86.3/lib:$LD_LIBRARY_PATH"
exec /nix/store/6334pjf6w2q623rgvwi499qaiym1p6yv-python3-3.11.14/bin/python -m uvicorn backend.main:app --host 0.0.0.0 --port 5000
