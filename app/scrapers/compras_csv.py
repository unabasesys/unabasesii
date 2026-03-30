"""
Wrapper para ejecutar el scraper de compras CSV (exportador.py local).
"""

import logging
from pathlib import Path

from playwright.sync_api import Playwright

from app.core.config import SII_HEADLESS
from app.scrapers.exportador import run as _run_compras

logger = logging.getLogger(__name__)


def run(
    playwright: Playwright,
    rut_usuario: str,
    clave_usuario: str,
    fecha: str,
    ruta_csv: str,
    headless: bool = SII_HEADLESS,
    hostname: str | None = None,
) -> None:
    """
    Descarga el CSV de compras del SII para un mes dado.

    Args:
        rut_usuario: RUT completo con guion (ej: 12345678-9)
        clave_usuario: Clave SII
        fecha: Fecha YYYY-MM-DD (se extrae el mes)
        ruta_csv: Ruta donde guardar el CSV descargado
        headless: Ejecutar sin ventana
        hostname: Hostname del cliente (solo para logs)
    """
    logger.info(
        "[COMPRAS-CSV] Descargando compras | rut=%s | fecha=%s | hostname=%s",
        rut_usuario, fecha, hostname or "(sin hostname)",
    )

    _run_compras(
        playwright=playwright,
        rut_usuario=rut_usuario,
        clave_usuario=clave_usuario,
        fecha=fecha,
        ruta_csv=ruta_csv,
        headless=headless,
    )

    logger.info("[COMPRAS-CSV] CSV descargado exitosamente | rut=%s", rut_usuario)
