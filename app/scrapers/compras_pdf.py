"""
Wrapper para ejecutar la descarga de PDFs de compras (newLogic.py local).
"""

import logging
from typing import Any, Dict

from playwright.sync_api import Playwright

from app.core.config import SII_HEADLESS
from app.scrapers.newLogic import download_and_upload_compras_pdfs

logger = logging.getLogger(__name__)


def run(
    playwright: Playwright,
    rut_empresa: str,
    clave: str,
    fecha: str,
    hostname: str,
    rut_apoderado: str | None = None,
    dv_apoderado: str | None = None,
    clave_apoderado: str | None = None,
    headless: bool = SII_HEADLESS,
    fecha_desde: str | None = None,
    fecha_hasta: str | None = None,
) -> Dict[str, Any]:
    """
    Descarga PDFs de compras del SII.

    Args:
        rut_empresa: RUT completo de la empresa (ej: 12345678-9)
        clave: Clave SII
        fecha: Fecha base YYYY-MM-DD
        hostname: Para subir PDFs y cache
        rut_apoderado: RUT apoderado (sin DV)
        dv_apoderado: DV del apoderado
        clave_apoderado: Clave del apoderado
        headless: Sin ventana
        fecha_desde: Override inicio periodo
        fecha_hasta: Override fin periodo
    """
    logger.info(
        "[COMPRAS-PDF] Iniciando descarga | rut_empresa=%s | rut_apoderado=%s-%s | hostname=%s",
        rut_empresa,
        rut_apoderado or "N/A",
        dv_apoderado or "N/A",
        hostname or "(sin hostname)",
    )

    result = download_and_upload_compras_pdfs(
        playwright=playwright,
        rut_usuario=rut_empresa,
        clave_usuario=clave,
        fecha=fecha,
        hostname=hostname,
        rut_apoderado=rut_apoderado,
        dv_apoderado=dv_apoderado,
        clave_apoderado=clave_apoderado,
        headless=headless,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
    )

    logger.info("[COMPRAS-PDF] Resultado: %s", result)
    return result
