"""
Modelos Pydantic para request/response del servicio SII.
Solo lo necesario para SII - cero dependencias con OpenAI u otros servicios.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ── Request ─────────────────────────────────────────────────────────────────
class SiiRequest(BaseModel):
    """Payload que envia el cliente para descargar documentos del SII."""

    rut: str = Field(..., description="RUT sin guion (ej: 12345678)")
    dv: str = Field(..., description="Digito verificador (ej: 9 o K)")
    fecha: str = Field(..., description="Fecha base YYYY-MM-DD")
    clave: str = Field(..., description="Clave SII del contribuyente")
    hostname: str = Field("", description="Hostname del cliente (para subir PDFs y cache)")

    # Apoderado: si el login se hace con un apoderado distinto al contribuyente
    rut_apoderado: Optional[str] = Field(None, description="RUT del apoderado")
    dv_apoderado: Optional[str] = Field(None, description="DV del apoderado")
    clave_apoderado: Optional[str] = Field(None, description="Clave del apoderado")

    # Opciones de descarga
    fecha_desde: Optional[str] = Field(None, description="Override fecha inicio YYYY-MM-DD")
    fecha_hasta: Optional[str] = Field(None, description="Override fecha fin YYYY-MM-DD")
    descargar_compras: bool = Field(True, description="Descargar CSV de compras")
    descargar_boletas: bool = Field(True, description="Descargar CSV/XLS de boletas")


# ── Response ────────────────────────────────────────────────────────────────
class SiiResponse(BaseModel):
    """Respuesta estandarizada del servicio SII."""

    status: str = "ok"
    documentos: List[Dict[str, Any]] = []
    total: int = 0
    metadata: Dict[str, Any] = {}
