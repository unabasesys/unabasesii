"""
Cache de PDFs ya descargados, indexado por hostname.

Guarda un JSON por hostname en downloads/_cache/ con la lista de filenames
que ya fueron subidos exitosamente, para no re-descargarlos.
"""

import json
import logging
import re
from pathlib import Path
from typing import Set

from app.core.config import PDF_CACHE_DIR

logger = logging.getLogger(__name__)


def _cache_path(hostname: str) -> Path:
    safe = re.sub(r"[^\w\-]", "_", (hostname or "sin_hostname").strip()) or "sin_hostname"
    return PDF_CACHE_DIR / f"descargados_{safe}.json"


def load_cache(hostname: str) -> Set[str]:
    path = _cache_path(hostname)
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return set(data) if isinstance(data, list) else set()
    except Exception as exc:
        logger.warning("No se pudo leer cache %s: %s", path, exc)
        return set()


def save_cache(hostname: str, cache: Set[str]) -> None:
    path = _cache_path(hostname)
    try:
        path.write_text(json.dumps(sorted(cache), ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("No se pudo guardar cache %s: %s", path, exc)
