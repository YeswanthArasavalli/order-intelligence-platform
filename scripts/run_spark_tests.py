"""Run focused Spark tests through spark-submit, using its actual JVM/Python."""

import sys
from pathlib import Path

import pytest

if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    targets = sys.argv[1:] or ["tests/unit/test_milestone2.py"]
    sys.exit(pytest.main([*[str(root / target) for target in targets], "-v", "--tb=short", "-p", "no:cacheprovider"]))
