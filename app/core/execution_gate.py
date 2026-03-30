"""
Gate de ejecucion serial para operaciones SII.

Playwright solo puede tener una sesion abierta a la vez contra el SII,
asi que este gate asegura que las operaciones foreground (requests HTTP)
tengan prioridad sobre las background (descarga de PDFs).
"""

import logging
import threading
import time
from contextlib import contextmanager
from typing import Literal

from app.core.config import BACKGROUND_GRACE_MS

logger = logging.getLogger(__name__)

ExecutionPriority = Literal["foreground", "background"]


class SiiExecutionGate:
    """Mutex con prioridad foreground > background."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._owner: int | None = None
        self._depth = 0
        self._fg_waiting = 0
        self._bg_waiting = 0

    # ── Estado para logs ────────────────────────────────────────────────
    def _state(self) -> dict[str, int]:
        return {
            "fg_waiting": self._fg_waiting,
            "bg_waiting": self._bg_waiting,
            "active": int(self._owner is not None),
        }

    # ── Acquire / Release ───────────────────────────────────────────────
    def acquire(self, priority: ExecutionPriority, operation: str, rut: str) -> float:
        ident = threading.get_ident()
        t0 = time.monotonic()
        grace_deadline: float | None = None

        with self._cond:
            # Re-entrante
            if self._owner == ident:
                self._depth += 1
                return 0.0

            attr = "_fg_waiting" if priority == "foreground" else "_bg_waiting"
            setattr(self, attr, getattr(self, attr) + 1)
            logger.info("[SII-GATE] Esperando | op=%s rut=%s prio=%s %s", operation, rut, priority, self._state())

            try:
                while True:
                    if self._owner is not None:
                        grace_deadline = None
                        self._cond.wait(timeout=1.0)
                        continue

                    if priority == "background" and self._fg_waiting > 0:
                        grace_deadline = None
                        self._cond.wait(timeout=1.0)
                        continue

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

            self._owner = ident
            self._depth = 1
            waited = time.monotonic() - t0
            logger.info("[SII-GATE] Adquirido | op=%s rut=%s prio=%s waited=%.2fs %s", operation, rut, priority, waited, self._state())
            return waited

    def release(self, operation: str, rut: str, priority: ExecutionPriority) -> None:
        with self._cond:
            if self._owner != threading.get_ident():
                raise RuntimeError("release() desde hilo no propietario")
            self._depth -= 1
            if self._depth == 0:
                self._owner = None
                logger.info("[SII-GATE] Liberado  | op=%s rut=%s prio=%s %s", operation, rut, priority, self._state())
                self._cond.notify_all()


# Singleton global
_GATE = SiiExecutionGate()


@contextmanager
def sii_serial_execution(operation: str, rut: str, priority: ExecutionPriority = "foreground"):
    """Context manager para ejecutar operaciones SII de forma serial."""
    _GATE.acquire(priority=priority, operation=operation, rut=rut)
    try:
        yield
    finally:
        _GATE.release(operation=operation, rut=rut, priority=priority)
