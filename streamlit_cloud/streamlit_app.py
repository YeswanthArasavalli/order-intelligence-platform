"""Streamlit Community Cloud entry point for the synthetic portfolio demo.

This thin script is the entire hosted application:

* ``OIP_DEMO_MODE`` is set to ``synthetic`` here, before the dashboard is
  imported, because Community Cloud offers no way to preset environment
  variables and an unset flag means real mode. The hosted demo is therefore
  always the clearly-labelled synthetic portfolio demo and can never reach the
  real PostgreSQL/Redis read path. The global default in
  ``dashboard.demo_provider.selected_mode`` is unchanged: every other launch
  path (``python -m dashboard.demo``, ``streamlit run dashboard/app.py``,
  scripted replay, real pipeline) keeps its existing behavior, and an invalid
  flag still fails loudly wherever it is set — this file simply never leaves
  the flag unset.
* The repository root is prepended to ``sys.path`` so the root-level packages
  (``dashboard``, ``storage``) import reliably when Streamlit launches only
  this file.
* Rendering, mode validation and the ``Synthetic demo data — not a live
  pipeline.`` disclosure are delegated to the unchanged ``dashboard/app.py``;
  nothing is duplicated here and no Kafka, Spark, MinIO, PostgreSQL, Redis or
  notification client is constructed in demo mode.

Dependencies: Community Cloud supports a requirements file in the repository
root or beside the entrypoint, but its documentation does not fix which one is
installed when both exist. ``streamlit_cloud/requirements.txt`` — a one-line
include of the authoritative root ``requirements-demo.txt`` — is placed beside
this entrypoint so the hosted app is intended to install only the demo set and
leave the full pipeline stack in root ``requirements.txt`` for local use. That
selection is not yet verified: the first Community Cloud build log must confirm
which requirements file was installed before the hosted dependency arrangement
can be called correct.
No secrets, credentials or ``.env`` file are required by this deployment.
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.demo_provider import DEMO_MODE_ENV_VAR, DEMO_MODE_VALUE  # noqa: E402

os.environ[DEMO_MODE_ENV_VAR] = DEMO_MODE_VALUE

from dashboard.app import main  # noqa: E402

main()
