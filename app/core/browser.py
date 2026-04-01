"""
Helpers para lanzar Chromium de forma estable en local y en contenedores.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict


def is_container_runtime() -> bool:
    return bool(
        os.getenv("RAILWAY_ENVIRONMENT")
        or os.getenv("RAILWAY_PROJECT_ID")
        or os.getenv("K_SERVICE")
        or Path("/.dockerenv").exists()
    )


def build_chromium_launch_kwargs(headless: bool, slow_mo: int = 0) -> Dict[str, Any]:
    args = ["--ignore-certificate-errors"]

    if is_container_runtime():
        # Railway/Docker suele necesitar menos sandbox y menos uso de /dev/shm.
        args.extend(
            [
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-setuid-sandbox",
                "--no-sandbox",
            ]
        )

    launch_kwargs: Dict[str, Any] = {
        "headless": headless,
        "slow_mo": slow_mo,
        "args": args,
    }

    if is_container_runtime():
        launch_kwargs["chromium_sandbox"] = False

    return launch_kwargs
