"""
Scheduler aislado para descarga de PDFs de compras.

Corre cada COMPRAS_PDF_INTERVAL_HOURS horas como un hilo daemon.
Obtiene las empresas desde lisboa.unabase.com y para cada una ejecuta
la logica de newLogic.py con su propia sesion de Chromium.

Usa sii_serial_execution con prioridad "background" para no interferir
con las peticiones foreground de sii-documentos.
"""

import logging
import threading
import time
from datetime import datetime

import requests
from playwright.sync_api import sync_playwright

from app.core.config import (
    COMPRAS_PDF_ENABLED,
    COMPRAS_PDF_INTERVAL_HOURS,
    LISBOA_API_URL,
    SII_HEADLESS,
)
from app.core.execution_gate import sii_serial_execution
from app.scrapers.newLogic import download_and_upload_compras_pdfs
from app.utils.rut import extract_rut_dv, extract_rut_number, formatear_rut

logger = logging.getLogger(__name__)

_SCHEDULER_THREAD: threading.Thread | None = None
_SCHEDULER_LOCK = threading.Lock()


def _limpiar_rut(rut_raw: str) -> str:
    """Extrae solo el numero del RUT (sin DV, sin puntos ni guion)."""
    return extract_rut_number(rut_raw)


def _obtener_dv(rut_raw: str) -> str:
    """Extrae el digito verificador del RUT."""
    return extract_rut_dv(rut_raw)


def _fetch_empresas() -> list[dict]:
    """Obtiene la lista de empresas desde lisboa.unabase.com."""
    try:
        resp = requests.get(LISBOA_API_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        empresas = data.get("data", [])
        if not isinstance(empresas, list):
            logger.warning("[COMPRAS-SCHEDULER] Respuesta inesperada de Lisboa: %s", type(empresas))
            return []
        return empresas
    except Exception as exc:
        logger.error("[COMPRAS-SCHEDULER] Error obteniendo empresas de Lisboa: %s", exc)
        return []


def _process_empresa(empresa: dict) -> None:
    """Procesa una empresa: descarga sus PDFs de compras."""
    hostname_raw = empresa.get("empresa", "")
    if not hostname_raw:
        return

    hostname = hostname_raw if hostname_raw.startswith("http") else f"https://{hostname_raw}"
    short_hostname = hostname_raw.replace("https://", "").replace("http://", "").replace(".unabase.com", "")

    rut_empresa_raw = empresa.get("rut", "")
    rut_empresa_num = _limpiar_rut(rut_empresa_raw)
    dv_empresa = _obtener_dv(rut_empresa_raw)

    if not rut_empresa_num or not dv_empresa:
        logger.warning("[COMPRAS-SCHEDULER] RUT empresa invalido para %s: %s", hostname_raw, rut_empresa_raw)
        return

    rut_empresa_completo = f"{rut_empresa_num}-{dv_empresa}"
    clave = empresa.get("clave", "")

    rut_apoderado_raw = empresa.get("rutApoderado", "")
    rut_apoderado = _limpiar_rut(rut_apoderado_raw)
    dv_apoderado = _obtener_dv(rut_apoderado_raw)
    clave_apoderado = empresa.get("claveApoderado", "")

    if not clave:
        logger.warning("[COMPRAS-SCHEDULER] Sin clave SII para %s, saltando", hostname_raw)
        return

    # Determinar el RUT que se usara para el login (apoderado tiene prioridad)
    if rut_apoderado and dv_apoderado and clave_apoderado:
        rut_login = f"{rut_apoderado}-{dv_apoderado}"
    else:
        rut_login = rut_empresa_completo

    hoy = datetime.now()
    fecha = hoy.strftime("%Y-%m-%d")

    logger.info(
        "[COMPRAS-SCHEDULER] Procesando empresa=%s | rut_empresa=%s | rut_login=%s",
        hostname_raw, rut_empresa_completo, rut_login,
    )

    try:
        logger.info(
            "[COMPRAS-SCHEDULER] ▶ Iniciando descarga de PDFs | hostname=%s | rut_empresa=%s | dv_empresa=%s | "
            "rut_apoderado=%s | dv_apoderado=%s | rut_login=%s | fecha=%s",
            hostname_raw, rut_empresa_num, dv_empresa,
            rut_apoderado or "(sin apoderado)", dv_apoderado or "-",
            rut_login, fecha,
        )
        with sii_serial_execution("compras_pdf_scheduler", rut_login, priority="background"):
            with sync_playwright() as pw:
                result = download_and_upload_compras_pdfs(
                    playwright=pw,
                    rut_usuario=rut_empresa_completo,
                    clave_usuario=clave,
                    fecha=fecha,
                    hostname=short_hostname,
                    rut_apoderado=rut_apoderado or None,
                    dv_apoderado=dv_apoderado or None,
                    clave_apoderado=clave_apoderado or None,
                    headless=SII_HEADLESS,
                )
                logger.info(
                    "[COMPRAS-SCHEDULER] Resultado empresa=%s: status=%s total=%s uploaded=%s",
                    hostname_raw,
                    result.get("status"),
                    result.get("total"),
                    result.get("uploaded"),
                )
    except Exception as exc:
        logger.error("[COMPRAS-SCHEDULER] Error procesando empresa %s: %s", hostname_raw, exc)


def _scheduler_loop() -> None:
    """Bucle principal del scheduler. Corre indefinidamente."""
    interval_s = COMPRAS_PDF_INTERVAL_HOURS * 3600
    logger.info(
        "[COMPRAS-SCHEDULER] Iniciado | intervalo=%.1fh (%.0fs) | lisboa=%s",
        COMPRAS_PDF_INTERVAL_HOURS, interval_s, LISBOA_API_URL,
    )

    while True:
        try:
            logger.info("[COMPRAS-SCHEDULER] === Inicio de ciclo ===")
            empresas = _fetch_empresas()
            logger.info("[COMPRAS-SCHEDULER] Empresas obtenidas: %d", len(empresas))

            for empresa in empresas:
                try:
                    _process_empresa(empresa)
                except Exception as exc:
                    logger.error(
                        "[COMPRAS-SCHEDULER] Error inesperado en empresa %s: %s",
                        empresa.get("empresa", "?"), exc,
                    )

            logger.info("[COMPRAS-SCHEDULER] === Ciclo completado. Proximo en %.1fh ===", COMPRAS_PDF_INTERVAL_HOURS)
        except Exception as exc:
            logger.error("[COMPRAS-SCHEDULER] Error en ciclo principal: %s", exc)

        time.sleep(interval_s)


def start_compras_pdf_scheduler() -> None:
    """Inicia el scheduler como hilo daemon si esta habilitado."""
    global _SCHEDULER_THREAD

    if not COMPRAS_PDF_ENABLED:
        logger.info("[COMPRAS-SCHEDULER] Deshabilitado por configuracion (COMPRAS_PDF_ENABLED=false)")
        return

    with _SCHEDULER_LOCK:
        if _SCHEDULER_THREAD is not None and _SCHEDULER_THREAD.is_alive():
            logger.info("[COMPRAS-SCHEDULER] Ya esta corriendo, no se inicia otro")
            return

        t = threading.Thread(
            target=_scheduler_loop,
            name="compras-pdf-scheduler",
            daemon=True,
        )
        t.start()
        _SCHEDULER_THREAD = t
        logger.info(
            "[COMPRAS-SCHEDULER] Hilo iniciado | intervalo=%.1fh | habilitado=%s",
            COMPRAS_PDF_INTERVAL_HOURS, COMPRAS_PDF_ENABLED,
        )
