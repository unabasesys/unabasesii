"""
Configuracion de logging para el servicio SII.
"""

import logging
import sys


def setup_logging(level: int = logging.INFO) -> None:
    fmt = "%(asctime)s | %(name)-30s | %(levelname)-7s | %(message)s"
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S"))

    root = logging.getLogger()
    root.setLevel(level)

    # Limpiar handlers previos para evitar duplicados en reloads
    root.handlers.clear()
    root.addHandler(handler)

    # Silenciar logs ruidosos de terceros
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("playwright").setLevel(logging.WARNING)
