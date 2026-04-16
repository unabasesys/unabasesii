"""
Punto de entrada del servicio SII.

Backend dedicado exclusivamente a operaciones del SII:
- Descarga de CSVs de compras y boletas
- Descarga de PDFs de compras y boletas en background
- Formateo de documentos para Node.js

Sin dependencia de OpenAI, procesamiento de imagenes, u otros servicios.
Proyecto 100% independiente.
"""

import logging

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import DEBUG, HOST, PORT
from app.core.logging import setup_logging
from app.api.sii_router import router as sii_router
from app.services.compras_pdf_scheduler import start_compras_pdf_scheduler

# Configurar logging antes de todo
setup_logging(level=logging.DEBUG if DEBUG else logging.INFO)
logger = logging.getLogger(__name__)

# ── App FastAPI ─────────────────────────────────────────────────────────────
app = FastAPI(
    title="SII Document Service",
    description="Servicio dedicado a descargar documentos del SII (compras, boletas, PDFs)",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(sii_router)


# ── Startup: iniciar scheduler de PDFs de compras ─────────────────────────

@app.on_event("startup")
async def _startup():
    start_compras_pdf_scheduler()


# ── Endpoints de salud ──────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "service": "SII Document Service",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "sii_documentos": "POST /api/sii-documentos",
            "health": "GET /health",
            "docs": "/docs",
        },
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


# ── Error handler global ───────────────────────────────────────────────────

@app.exception_handler(Exception)
async def global_error_handler(request, exc):
    logger.error("Excepcion no manejada: %s", exc)
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "detail": str(exc) if DEBUG else "Error interno del servidor",
        },
    )


# ── Entrypoint ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("Iniciando SII Document Service en %s:%d", HOST, PORT)
    uvicorn.run(
        "app.main:app",
        host=HOST,
        port=PORT,
        reload=False,
        log_level="info",
    )
