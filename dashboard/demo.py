"""One-command launcher for the synthetic dashboard demo (Windows-friendly).

    python -m dashboard.demo

Sets ``OIP_DEMO_MODE=synthetic`` for the Streamlit child process and starts the
unchanged dashboard. No Kafka, Spark, MinIO, PostgreSQL, Redis, credentials or
``.env`` file are required; see ``dashboard/demo_provider.py`` for the sample and
``requirements-demo.txt`` for the minimal dependency set.
"""

import os
from pathlib import Path
import subprocess
import sys

from dashboard.demo_provider import DEMO_MODE_ENV_VAR, DEMO_MODE_VALUE

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "dashboard" / "app.py"


def streamlit_command() -> list[str]:
    """Streamlit invocation for this interpreter, resolved without a shell."""
    return [sys.executable, "-m", "streamlit", "run", str(APP)]


def launch_environment() -> dict[str, str]:
    """Explicit demo selection plus the repository root on PYTHONPATH.

    Streamlit puts the script directory on ``sys.path``; the package imports in
    ``dashboard/app.py`` need the repository root, which a bare ``streamlit run``
    only picks up from the current working directory.
    """
    environment = dict(os.environ)
    environment[DEMO_MODE_ENV_VAR] = DEMO_MODE_VALUE
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = str(ROOT) + (os.pathsep + existing if existing else "")
    return environment


def main() -> int:
    print(f"{DEMO_MODE_ENV_VAR}={DEMO_MODE_VALUE}: synthetic demo mode — not a live pipeline.", flush=True)
    print("Press Ctrl+C in this window to stop the dashboard.", flush=True)
    return subprocess.call(streamlit_command(), env=launch_environment())


if __name__ == "__main__":
    sys.exit(main())
