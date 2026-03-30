"""
Router de la API SII.

Un solo endpoint: POST /api/sii-documentos
Sin dependencia de OpenAI, sin logica de documentos no-SII.
"""

import logging

from fastapi import APIRouter, HTTPException

from app.models.schemas import SiiRequest, SiiResponse
from app.services.sii_orchestrator import process_sii_request

logger = logging.getLogger(__name__)

router = APIRouter(tags=["SII"])


@router.post("/api/sii-documentos", response_model=SiiResponse)
async def get_sii_documents(request: SiiRequest):
    """
    Descarga y procesa documentos del SII (compras y boletas).

    - Descarga CSVs de compras y boletas en paralelo
    - Parsea y formatea la respuesta
    - Encola descarga de PDFs en segundo plano (si hostname presente)

    No tiene dependencia de OpenAI ni otros servicios externos.
    """
    # Validar campos requeridos
    if not request.rut or not request.dv or not request.fecha or not request.clave:
        raise HTTPException(
            status_code=400,
            detail="Faltan campos requeridos: rut, dv, fecha, clave",
        )

    try:
        result = await process_sii_request(request)
        return result
    except Exception as exc:
        logger.exception("[API] Error procesando sii-documentos: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
