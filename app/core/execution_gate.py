"""
Gate de ejecucion para operaciones SII.

Permite multiples sesiones concurrentes contra el SII (hasta MAX_CONCURRENT_SESSIONS),
con dos restricciones:
  1. Un mismo RUT no puede tener dos sesiones simultaneas (el SII lo rechaza).
  2. Las operaciones foreground (requests HTTP) tienen prioridad sobre background (PDFs).

Antes era un mutex global que serializaba TODO. Ahora RUTs distintos corren en paralelo.
"""

import logging
import threading
import time
from contextlib import contextmanager
from typing import Literal

from app.core.config import BACKGROUND_GRACE_MS, MAX_CONCURRENT_SESSIONS

logger = logging.getLogger(__name__)

ExecutionPriority = Literal["foreground", "background"]


class SiiExecutionGate:
    """Semaforo con limite global + lock por RUT + prioridad foreground > background."""

    def __init__(self, max_sessions: int) -> None:
        self._cond = threading.Condition()
        self._max_sessions = max_sessions
        # RUTs actualmente ejecutandose → set de thread idents
        self._active_ruts: dict[str, int] = {}  # rut → thread ident
        self._fg_waiting = 0
        self._bg_waiting = 0

    # ── Estado para logs ────────────────────────────────────────────────
    def _state(self) -> dict[str, object]:
        return {
            "active": len(self._active_ruts),
            "max": self._max_sessions,
            "ruts": list(self._active_ruts.keys()),
            "fg_waiting": self._fg_waiting,
            "bg_waiting": self._bg_waiting,
        }

    # ── Acquire / Release ───────────────────────────────────────────────
    def acquire(self, priority: ExecutionPriority, operation: str, rut: str) -> float:
        ident = threading.get_ident()
        t0 = time.monotonic()
        grace_deadline: float | None = None
        rut_key = rut.replace("-", "").upper()

        with self._cond:
            # Re-entrante: mismo hilo ya tiene este RUT
            if self._active_ruts.get(rut_key) == ident:
                return 0.0

            attr = "_fg_waiting" if priority == "foreground" else "_bg_waiting"
            setattr(self, attr, getattr(self, attr) + 1)
            logger.info(
                "[SII-GATE] Esperando | op=%s rut=%s prio=%s %s",
                operation, rut, priority, self._state(),
            )

            try:
                while True:
                    # Condicion 1: el mismo RUT ya esta activo (otro hilo)
                    if rut_key in self._active_ruts:
                        grace_deadline = None
                        self._cond.wait(timeout=1.0)
                        continue

                    # Condicion 2: ya se alcanzo el maximo de sesiones concurrentes
                    if len(self._active_ruts) >= self._max_sessions:
                        grace_deadline = None
                        self._cond.wait(timeout=1.0)
                        continue

                    # Condicion 3: background cede ante foreground esperando
                    if priority == "background" and self._fg_waiting > 0:
                        grace_deadline = None
                        self._cond.wait(timeout=1.0)
                        continue

                    # Condicion 4: grace period para background
                    if priority == "background" and BACKGROUND_GRACE_MS > 0:
                        if grace_deadline is None:
                            grace_deadline = time.monotonic() + BACKGROUND_GRACE_MS / 1000
                        remaining = grace_deadline - time.monotonic()
                        if remaining > 0:
                            self._cond.wait(timeout=min(0.25, remaining))
                            continue

                    break
            finally:
                setattr(self, attr, getattr(self, attr) - 1)

            self._active_ruts[rut_key] = ident
            waited = time.monotonic() - t0
            logger.info(
                "[SII-GATE] Adquirido | op=%s rut=%s prio=%s waited=%.2fs %s",
                operation, rut, priority, waited, self._state(),
            )
            return waited

    def release(self, operation: str, rut: str, priority: ExecutionPriority) -> None:
        rut_key = rut.replace("-", "").upper()
        with self._cond:
            if self._active_ruts.get(rut_key) != threading.get_ident():
                raise RuntimeError(f"release() desde hilo no propietario (rut={rut})")
            del self._active_ruts[rut_key]
            logger.info(
                "[SII-GATE] Liberado  | op=%s rut=%s prio=%s %s",
                operation, rut, priority, self._state(),
            )
            self._cond.notify_all()


# Singleton global
_GATE = SiiExecutionGate(max_sessions=MAX_CONCURRENT_SESSIONS)


@contextmanager
def sii_serial_execution(operation: str, rut: str, priority: ExecutionPriority = "foreground"):
    """Context manager para ejecutar operaciones SII con concurrencia controlada."""
    _GATE.acquire(priority=priority, operation=operation, rut=rut)
    try:
        yield
    finally:
        _GATE.release(operation=operation, rut=rut, priority=priority)
