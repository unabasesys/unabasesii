"""
Configuracion centralizada del servicio SII.
Todas las variables de entorno se leen aqui una sola vez.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Server ──────────────────────────────────────────────────────────────────
PORT: int = int(os.getenv("PORT", "8001"))
HOST: str = os.getenv("HOST", "0.0.0.0")
DEBUG: bool = os.getenv("DEBUG", "false").strip().lower() in {"1", "true", "yes"}

# ── SII / Playwright ───────────────────────────────────────────────────────
SII_HEADLESS: bool = os.getenv("SII_HEADLESS", "true").strip().lower() in {"1", "true", "yes", "on"}
PDF_QUEUE_MAXSIZE: int = max(int(os.getenv("SII_PDF_QUEUE_MAXSIZE", "24")), 1)
BACKGROUND_GRACE_MS: int = max(int(os.getenv("SII_BACKGROUND_GRACE_MS", "1500")), 0)
MAX_CONCURRENT_SESSIONS: int = max(int(os.getenv("SII_MAX_CONCURRENT_SESSIONS", "3")), 1)

# ── URLs externas ──────────────────────────────────────────────────────────
SAVE_PDF_URL: str = os.getenv("SAVE_PDF_URL", "https://frank.unabase.com/node/savePdfDocumento")
SII_BASE_URL: str = "https://www1.sii.cl"
SII_DOCS_URL: str = "https://www1.sii.cl/cgi-bin/Portal001/mipeLaunchPage.cgi?OPCION=1&TIPO=4"
SII_SERVICIOS_URL: str = "https://www.sii.cl/servicios_online/1039-.html"

# ── Directorios de trabajo ─────────────────────────────────────────────────
BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent
TEMP_DIR: Path = BASE_DIR / "_temp"
DOWNLOADS_DIR: Path = BASE_DIR / "downloads"
PDF_CACHE_DIR: Path = DOWNLOADS_DIR / "_cache"

TEMP_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ── Timeouts ───────────────────────────────────────────────────────────────
SCRAPER_TIMEOUT_S: int = 600  # 10 min para cada descarga de CSV
DEFAULT_TIMEZONE: str = "America/Santiago"

# ── Scheduler de PDFs de compras (proceso aislado) ────────────────────────
COMPRAS_PDF_INTERVAL_HOURS: float = float(os.getenv("COMPRAS_PDF_INTERVAL_HOURS", "6"))
COMPRAS_PDF_ENABLED: bool = os.getenv("COMPRAS_PDF_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
LISBOA_API_URL: str = os.getenv("LISBOA_API_URL", "https://lisboa.unabase.com/node/app/sii/list")
