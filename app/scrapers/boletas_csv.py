"""
Wrapper para ejecutar el scraper de boletas (boleta.py local).
"""

import logging
from pathlib import Path

from playwright.sync_api import Playwright

from app.core.config import DEFAULT_TIMEZONE, SII_HEADLESS
from app.scrapers.boleta import run as _run_boletas

logger = logging.getLogger(__name__)


def run(
    playwright: Playwright,
    rut: str,
    dv: str,
    clave: str,
    fecha: str,
    output_file: Path,
    headless: bool = SII_HEADLESS,
    hostname: str = "",
    pdf_dir: Path | None = None,
    descargar_pdfs: bool = False,
) -> None:
    """
    Ejecuta el scraper de boletas.

    Args:
        rut: RUT sin guion
        dv: Digito verificador
        clave: Clave SII
        fecha: Fecha YYYY-MM-DD
        output_file: Ruta para guardar XLS/CSV
        headless: Sin ventana
        hostname: Para subir PDFs
        pdf_dir: Directorio para PDFs de boletas
        descargar_pdfs: Si descargar PDFs ademas del CSV
    """
    logger.info(
        "[BOLETAS] Ejecutando scraper | rut=%s-%s | fecha=%s | pdfs=%s | hostname=%s",
        rut, dv, fecha, descargar_pdfs, hostname or "(sin hostname)",
    )

    _run_boletas(
        playwright=playwright,
        rut=rut,
        clave=clave,
        out_path=output_file,
        headless=headless,
        fecha=fecha,
        dv=dv,
        reintentos_error_sii=3,
        reintentos_click=3,
        backoff_base_ms=700,
        timezone=DEFAULT_TIMEZONE,
        hostname=hostname,
        pdf_dir=pdf_dir,
        descargar_pdfs=descargar_pdfs,
    )

    logger.info("[BOLETAS] Scraper completado | rut=%s-%s", rut, dv)
