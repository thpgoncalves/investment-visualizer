from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STREAMLIT_APP_PATH = PROJECT_ROOT / "app" / "streamlit_app.py"


def iv_app() -> None:
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(STREAMLIT_APP_PATH)],
        cwd=PROJECT_ROOT,
        check=True,
    )


def iv_run() -> None:
    from pipelines.run_pipeline import run_pipeline

    run_pipeline()

    logger.info("🖥️ STREAMLIT | Starting application")
    iv_app()
