"""
Orquestador principal de descargas SII.

Coordina la descarga de compras y boletas en paralelo (cada una en su propio thread),
parsea los CSVs generados, formatea la respuesta y encola los PDFs en segundo plano.

Este es el unico modulo que conoce el flujo completo - cada pieza individual
(scraper, parser, formatter, pipeline) es independiente.
"""

import asyncio
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

from app.core.config import BASE_DIR, SCRAPER_TIMEOUT_S, SII_HEADLESS, TEMP_DIR
from app.core.execution_gate import sii_serial_execution
from app.models.schemas import SiiRequest
from app.services.csv_parser import parse_csv
from app.services.pdf_pipeline import schedule_pdfs
from app.utils.files import build_job_dir
from app.utils.formatters import build_sii_response

logger = logging.getLogger(__name__)


# ── Helpers sincronos (ejecutados en threads) ───────────────────────────────

def _run_compras_sync(
    rut_completo: str,
    clave: str,
    fecha: str,
    output_file: Path,
    headless: bool,
    hostname: str,
) -> None:
    logger.info("[ORQUESTADOR] Compras CSV | rut=%s | hostname=%s", rut_completo, hostname or "")

    with sii_serial_execution("compras_csv", rut_completo, priority="foreground"):
        env = os.environ.copy()
        env.setdefault("PYTHONUNBUFFERED", "1")
        command = [
            sys.executable,
            "-m",
            "app.scrapers.exportador",
            "--rut",
            rut_completo,
            "--clave",
            clave,
            "--fecha",
            fecha,
            "--csv",
            str(output_file),
        ]
        if headless:
            command.append("--headless")

        subprocess.run(
            command,
            cwd=str(BASE_DIR),
            env=env,
            check=True,
            timeout=SCRAPER_TIMEOUT_S,
        )


def _run_boletas_sync(
    rut: str,
    dv: str,
    clave: str,
    fecha: str,
    output_file: Path,
    headless: bool,
    hostname: str,
) -> None:
    logger.info("[ORQUESTADOR] Boletas CSV | rut=%s-%s | hostname=%s", rut, dv, hostname or "")

    with sii_serial_execution("boletas_csv", f"{rut}-{dv}", priority="foreground"):
        env = os.environ.copy()
        env.setdefault("PYTHONUNBUFFERED", "1")
        command = [
            sys.executable,
            "-m",
            "app.scrapers.boleta",
            "--rut",
            rut,
            "--dv",
            dv,
            "--clave",
            clave,
            "--fecha",
            fecha,
            "--out",
            str(output_file),
            "--reintentos-sii",
            "3",
            "--reintentos-click",
            "3",
            "--backoff-base-ms",
            "700",
            "--timezone",
            "America/Santiago",
        ]
        if headless:
            command.append("--headless")
        subprocess.run(
            command,
            cwd=str(BASE_DIR),
            env=env,
            check=True,
            timeout=SCRAPER_TIMEOUT_S,
        )


async def _wait_for_file(path: Path, timeout_s: float = 2.5) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists() and path.stat().st_size > 0:
            return True
        await asyncio.sleep(0.1)
    return path.exists() and path.stat().st_size > 0


# ── Descarga individual con manejo de errores ──────────────────────────────

async def _download_compras(
    rut: str, dv: str, fecha: str, clave: str, hostname: str,
) -> tuple[List[Dict[str, Any]], bool]:
    """Descarga y parsea compras. Retorna (documentos, exito)."""
    rut_completo = f"{rut}-{dv}"
    job_dir = build_job_dir(TEMP_DIR, "compras", rut, dv, hostname)
    output_file = job_dir / "registro_compras.csv"

    try:
        await asyncio.to_thread(
            _run_compras_sync, rut_completo, clave, fecha,
            output_file, SII_HEADLESS, hostname,
        )

        csv_path = output_file
        if await _wait_for_file(csv_path):
            docs = parse_csv(csv_path, "compras")
            logger.info("[ORQUESTADOR] Compras: %d documentos parseados", len(docs))
            return docs, True

        logger.warning("[ORQUESTADOR] CSV de compras no generado: %s", csv_path)
        return [], False

    except subprocess.TimeoutExpired:
        logger.error("[ORQUESTADOR] Timeout descargando compras (%ds)", SCRAPER_TIMEOUT_S)
        return [], False
    except subprocess.CalledProcessError as exc:
        logger.error("[ORQUESTADOR] Compras termino con error (exit=%s)", exc.returncode)
        return [], False
    except Exception as exc:
        logger.error("[ORQUESTADOR] Error descargando compras: %s", exc)
        return [], False


async def _download_boletas(
    rut: str, dv: str, fecha: str, clave: str, hostname: str,
) -> tuple[List[Dict[str, Any]], bool]:
    """Descarga y parsea boletas. Retorna (documentos, exito)."""
    job_dir = build_job_dir(TEMP_DIR, "boletas", rut, dv, hostname)
    output_file = job_dir / "registro_boletas.xls"

    try:
        await asyncio.to_thread(
            _run_boletas_sync, rut, dv, clave, fecha,
            output_file, SII_HEADLESS, hostname,
        )

        csv_path = output_file.with_suffix(".csv")
        if await _wait_for_file(csv_path):
            docs = parse_csv(csv_path, "boletas")
            logger.info("[ORQUESTADOR] Boletas: %d documentos parseados", len(docs))
            return docs, True

        logger.warning("[ORQUESTADOR] CSV de boletas no generado: %s", csv_path)
        return [], False

    except subprocess.TimeoutExpired:
        logger.error("[ORQUESTADOR] Timeout descargando boletas (%ds)", SCRAPER_TIMEOUT_S)
        return [], False
    except subprocess.CalledProcessError as exc:
        logger.error("[ORQUESTADOR] Boletas termino con error (exit=%s)", exc.returncode)
        return [], False
    except Exception as exc:
        logger.error("[ORQUESTADOR] Error descargando boletas: %s", exc)
        return [], False


# ── Funcion principal ───────────────────────────────────────────────────────

async def process_sii_request(request: SiiRequest) -> Dict[str, Any]:
    """
    Orquesta la descarga completa de documentos SII.

    1. Descarga compras y boletas en paralelo (cada una en su thread)
    2. Parsea los CSVs resultantes
    3. Formatea la respuesta para Node.js
    4. Encola descarga de PDFs en segundo plano (si hay hostname)

    Returns:
        Respuesta formateada compatible con SiiResponse.
    """
    rut, dv, fecha, clave = request.rut, request.dv, request.fecha, request.clave
    hostname = request.hostname or ""

    logger.info(
        "[ORQUESTADOR] Inicio | rut=%s-%s | fecha=%s | hostname=%s | compras=%s | boletas=%s",
        rut, dv, fecha, hostname or "(sin hostname)",
        request.descargar_compras, request.descargar_boletas,
    )

    t0 = time.monotonic()

    # ── Descarga paralela ───────────────────────────────────────────────
    tasks = []
    if request.descargar_compras:
        tasks.append(_download_compras(rut, dv, fecha, clave, hostname))
    else:
        tasks.append(_noop_download())

    if request.descargar_boletas:
        tasks.append(_download_boletas(rut, dv, fecha, clave, hostname))
    else:
        tasks.append(_noop_download())

    results = await asyncio.gather(*tasks)

    compras_raw, compras_ok = results[0]
    boletas_raw, boletas_ok = results[1]

    elapsed = time.monotonic() - t0
    logger.info(
        "[ORQUESTADOR] Descargas completadas en %.1fs | compras=%d(ok=%s) | boletas=%d(ok=%s)",
        elapsed, len(compras_raw), compras_ok, len(boletas_raw), boletas_ok,
    )

    # ── Formatear respuesta ─────────────────────────────────────────────
    extra_meta: Dict[str, Any] = {"elapsed_s": round(elapsed, 2)}

    # ── Encolar PDFs en background ──────────────────────────────────────
    if hostname and (compras_ok or boletas_ok):
        pdf_status = schedule_pdfs(
            rut=rut,
            dv=dv,
            fecha=fecha,
            clave=clave,
            hostname=hostname,
            rut_apoderado=request.rut_apoderado,
            dv_apoderado=request.dv_apoderado,
            clave_apoderado=request.clave_apoderado,
            headless=True,
            run_compras_pdfs=compras_ok,
            run_boletas_pdfs=boletas_ok,
        )
        extra_meta["pdf_pipeline"] = pdf_status
        logger.info(
            "[ORQUESTADOR] PDFs encolados | rut=%s-%s | compras=%s boletas=%s | status=%s",
            rut, dv, compras_ok, boletas_ok, pdf_status,
        )

    return build_sii_response(compras_raw, boletas_raw, extra_meta)


async def _noop_download() -> tuple[list, bool]:
    return [], True
