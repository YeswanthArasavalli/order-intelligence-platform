"""Shared pytest configuration for the local Spark test suites.

PySpark launches its Python workers through an executable named
``PYSPARK_PYTHON``. Upstream Spark defaults this to the literal ``python3``,
which is not present on a default Windows install, so a local Spark job dies
with ``Cannot run program "python3"`` before any test assertion is reached.

``scripts/verify.py`` already pins the interpreter for ``python main.py verify``,
but a direct ``pytest tests/unit/...`` invocation (a documented command) bypasses
that entry point. Pinning here makes every pytest path deterministic on all
platforms (Linux CI included) without touching any assertion. ``setdefault``
keeps an explicit operator override authoritative.
"""

import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
