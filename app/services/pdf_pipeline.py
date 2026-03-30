"""
Pipeline de descarga de PDFs en segundo plano.

Usa una cola con un worker thread para procesar descargas de PDFs
de boletas y compras de forma serial (el SII no permite sesiones
concurrentes).
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from queue import Full, Queue
from typing import Any, Dict

from playwright.sync_api import sync_playwright

from app.core.config import PDF_QUEUE_MAXSIZE, SII_HEADLESS, TEMP_DIR
from app.core.execution_gate import sii_serial_execution
from app.utils.files import build_job_dir

logger = logging.getLogger(__name__)

# ── Estado global del pipeline ──────────────────────────────────────────────
_STATE_LOCK = threading.RLock()
_QUEUE: Queue["PdfJob"] = Queue(maxsize=PDF_QUEUE_MAXSIZE)
_PENDING_KEYS: set[tuple] = set()
_RUNNING_KEYS: set[tuple] = set()
_WORKER: threading.Thread | None = None


@dataclass(frozen=True)
class PdfJob:
    key: tuple
    payload: dict[str, Any]
    enqueued_at: float = field(default_factory=time.monotonic)


def _state_snapshot() -> dict[str, int]:
    with _STATE_LOCK:
        return {
            "pending_jobs": len(_PENDING_KEYS),
            "running_jobs": len(_RUNNING_KEYS),
            "capacity": PDF_QUEUE_MAXSIZE,
        }


def _build_job_key(p: dict) -> tuple:
    return (
        p.get("hostname", ""),
        p.get("rut", ""),
        p.get("dv", ""),
        p.get("fecha", ""),
        (p.get("rut_apoderado") or "").strip(),
        (p.get("dv_apoderado") or "").strip(),
        bool(p.get("run_compras_pdfs", True)),
        bool(p.get("run_boletas_pdfs", True)),
    )


# ── Worker loop ─────────────────────────────────────────────────────────────
def _worker_loop() -> None:
    while True:
        job = _QUEUE.get()

        with _STATE_LOCK:
            _PENDING_KEYS.discard(job.key)
            _RUNNING_KEYS.add(job.key)

        p = job.payload
        logger.info(
            "[PDF-PIPELINE] Iniciando job | rut=%s-%s | fecha=%s | hostname=%s | espera=%.1fs",
            p["rut"], p["dv"], p["fecha"], p.get("hostname", ""), time.monotonic() - job.enqueued_at,
        )

        try:
            _execute_pdf_job(p)
        except Exception:
            logger.exception("[PDF-PIPELINE] Error en job %s-%s", p["rut"], p["dv"])
        finally:
            with _STATE_LOCK:
                _RUNNING_KEYS.discard(job.key)
            logger.info("[PDF-PIPELINE] Job finalizado | rut=%s-%s | %s", p["rut"], p["dv"], _state_snapshot())
            _QUEUE.task_done()


def _execute_pdf_job(p: dict) -> None:
    rut = p["rut"]
    dv = p["dv"]
    rut_completo = f"{rut}-{dv}"

    # 1. Boletas PDFs
    if p.get("run_boletas_pdfs", True):
        try:
            _run_boletas_pdfs(
                rut=rut, dv=dv, clave=p["clave"], fecha=p["fecha"],
                hostname=p.get("hostname", ""), headless=p.get("headless", SII_HEADLESS),
            )
        except Exception as exc:
            logger.warning("[PDF-PIPELINE] Error boletas PDFs %s: %s", rut_completo, exc)

    # 2. Compras PDFs
    if p.get("run_compras_pdfs", True):
        try:
            _run_compras_pdfs(
                rut=rut, dv=dv, clave=p["clave"], fecha=p["fecha"],
                hostname=p.get("hostname", ""),
                rut_apoderado=p.get("rut_apoderado"),
                dv_apoderado=p.get("dv_apoderado"),
                clave_apoderado=p.get("clave_apoderado"),
                headless=p.get("headless", SII_HEADLESS),
            )
        except Exception as exc:
            logger.warning("[PDF-PIPELINE] Error compras PDFs %s: %s", rut_completo, exc)


def _run_boletas_pdfs(rut: str, dv: str, clave: str, fecha: str, hostname: str, headless: bool) -> None:
    from app.scrapers.boletas_csv import run as run_boletas

    rut_completo = f"{rut}-{dv}"
    job_dir = build_job_dir(TEMP_DIR, "boletas_pdf", rut, dv, hostname)
    output_file = job_dir / "registro_boletas.xls"
    pdf_dir = job_dir / "boletas"
    pdf_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "[PDF-PIPELINE] Boletas PDFs | rut_login=%s | hostname=%s",
        rut_completo, hostname or "(sin hostname)",
    )

    with sii_serial_execution("boletas_pdf", rut_completo, priority="background"):
        with sync_playwright() as pw:
            run_boletas(
                playwright=pw, rut=rut, dv=dv, clave=clave, fecha=fecha,
                output_file=output_file, headless=headless, hostname=hostname,
                pdf_dir=pdf_dir, descargar_pdfs=True,
            )

    logger.info("[PDF-PIPELINE] Boletas PDFs completado | rut=%s", rut_completo)


def _run_compras_pdfs(
    rut: str, dv: str, clave: str, fecha: str, hostname: str,
    rut_apoderado: str | None, dv_apoderado: str | None, clave_apoderado: str | None,
    headless: bool,
) -> None:
    rut_completo = f"{rut}-{dv}"

    # Validar datos de apoderado
    missing = []
    if not (rut_apoderado or "").strip():
        missing.append("rut_apoderado")
    if not (dv_apoderado or "").strip():
        missing.append("dv_apoderado")
    if not (clave_apoderado or "").strip():
        missing.append("clave_apoderado")

    if missing:
        logger.warning(
            "[PDF-PIPELINE] Saltando compras PDFs para %s: faltan %s",
            rut_completo, ", ".join(missing),
        )
        return

    from app.scrapers.compras_pdf import run as run_compras_pdf

    logger.info(
        "[PDF-PIPELINE] Compras PDFs | rut_empresa=%s | rut_apoderado=%s-%s | hostname=%s",
        rut_completo, rut_apoderado, dv_apoderado, hostname or "(sin hostname)",
    )

    with sii_serial_execution("compras_pdf", rut_completo, priority="background"):
        with sync_playwright() as pw:
            run_compras_pdf(
                playwright=pw, rut_empresa=rut_completo, clave=clave, fecha=fecha,
                hostname=hostname, rut_apoderado=rut_apoderado, dv_apoderado=dv_apoderado,
                clave_apoderado=clave_apoderado, headless=headless,
            )

    logger.info("[PDF-PIPELINE] Compras PDFs completado | rut=%s", rut_completo)


# ── API publica ─────────────────────────────────────────────────────────────
def _ensure_worker() -> None:
    global _WORKER
    with _STATE_LOCK:
        if _WORKER is not None and _WORKER.is_alive():
            return
        w = threading.Thread(target=_worker_loop, name="sii-pdf-worker", daemon=True)
        w.start()
        _WORKER = w
        logger.info("[PDF-PIPELINE] Worker iniciado | capacidad=%d", PDF_QUEUE_MAXSIZE)


def schedule_pdfs(
    rut: str,
    dv: str,
    fecha: str,
    clave: str,
    hostname: str,
    rut_apoderado: str | None = None,
    dv_apoderado: str | None = None,
    clave_apoderado: str | None = None,
    headless: bool = SII_HEADLESS,
    run_compras_pdfs: bool = True,
    run_boletas_pdfs: bool = True,
) -> Dict[str, Any]:
    """
    Encola un job de descarga de PDFs en segundo plano.

    Returns:
        Dict con scheduled, reason y estado de la cola.
    """
    if not (run_compras_pdfs or run_boletas_pdfs):
        return {"scheduled": False, "reason": "disabled", **_state_snapshot()}

    _ensure_worker()

    payload = {
        "rut": rut, "dv": dv, "fecha": fecha, "clave": clave,
        "hostname": hostname, "rut_apoderado": rut_apoderado,
        "dv_apoderado": dv_apoderado, "clave_apoderado": clave_apoderado,
        "headless": headless, "run_compras_pdfs": run_compras_pdfs,
        "run_boletas_pdfs": run_boletas_pdfs,
    }
    job_key = _build_job_key(payload)
    job = PdfJob(key=job_key, payload=payload)

    with _STATE_LOCK:
        if job_key in _PENDING_KEYS or job_key in _RUNNING_KEYS:
            logger.info("[PDF-PIPELINE] Job duplicado omitido | rut=%s-%s", rut, dv)
            return {"scheduled": False, "reason": "duplicate", **_state_snapshot()}

        try:
            _QUEUE.put_nowait(job)
        except Full:
            logger.warning("[PDF-PIPELINE] Cola llena | rut=%s-%s", rut, dv)
            return {"scheduled": False, "reason": "queue_full", **_state_snapshot()}

        _PENDING_KEYS.add(job_key)
        logger.info("[PDF-PIPELINE] Job encolado | rut=%s-%s | %s", rut, dv, _state_snapshot())
        return {"scheduled": True, "reason": "enqueued", **_state_snapshot()}
