#!/bin/sh
# Failure-fixture interpreter from the declared Python 3.11 base, with its own loader.
export PYTHONHOME=/worker
exec /worker/ld-linux-x86-64.so.2 --library-path /worker/lib /worker/bin/python3.11 "$@"
