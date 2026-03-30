"""
Utilidades de archivos y nombres seguros.
"""

import re
import uuid
from datetime import datetime
from pathlib import Path


def safe_filename(text: str, maxlen: int = 40) -> str:
    return re.sub(r"[^\w\-]", "_", text.strip())[:maxlen]


def build_job_dir(base_dir: Path, tipo: str, rut: str, dv: str, hostname: str | None = None) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    job_id = f"{tipo}_{rut}{dv}_{stamp}_{uuid.uuid4().hex[:8]}"
    job_dir = base_dir / "jobs" / (hostname or "default") / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    return job_dir
