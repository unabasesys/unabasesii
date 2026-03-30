"""
Formateo de documentos SII al formato esperado por Node.js.
"""

import re
from typing import Any, Dict, List


# ── Mapeo de otros impuestos ────────────────────────────────────────────────
def map_otro_impuesto_nombre(codigo_oi: Any, etiqueta: Any = "") -> str:
    s = str(codigo_oi or "").strip().lower()
    etiqueta_s = str(etiqueta or "").strip().lower()

    try:
        code = int(re.sub(r"\D", "", s)) if s else None
    except Exception:
        code = None

    if code == 28:
        return "imp_combustible"
    if code in {25, 26, 27, 271}:
        return "imp_ila"

    combined = s + " " + etiqueta_s
    if any(k in combined for k in ["combust", "iec", "diesel", "bencina", "gasolin", "copec"]):
        return "imp_combustible"
    if any(k in combined for k in ["ila", "alcohol"]):
        return "imp_ila"
    if code is not None and 271 <= code <= 279:
        return "imp_combustible" if code != 271 else "imp_ila"

    return "otro_impuesto"


# ── Conversion de numeros formato chileno ───────────────────────────────────
def to_num(value: Any) -> float:
    if not value or value == "":
        return 0.0
    try:
        s = str(value).strip()
        s = s.replace(".", "").replace(",", ".").replace(" ", "")
        s = s.replace("$", "").replace("CLP", "").replace("USD", "")
        return float(s)
    except (ValueError, TypeError):
        return 0.0


# ── Mapeo de tipos de documento ─────────────────────────────────────────────
_TIPO_MAP = {
    "33": "factura",
    "34": "factura exenta",
    "66": "boleta",
    "52": "guia de despacho",
    "61": "nota de credito",
    "56": "nota de debito",
    "boleta_honorarios": "boleta",
}

_CODIGO_MAP = {
    "boleta_honorarios": 66,
    "boleta": 66,
    "factura": 33,
    "factura exenta": 34,
    "guia de despacho": 52,
    "nota de credito": 61,
    "nota de debito": 56,
}


def map_tipo_documento(tipo_doc: str) -> str:
    key = str(tipo_doc).lower().strip()
    return _TIPO_MAP.get(tipo_doc, _TIPO_MAP.get(key, key or "documento"))


def map_codigo_tipo_documento(tipo_doc: str) -> int:
    key = str(tipo_doc).lower().strip()
    if key in _CODIGO_MAP:
        return _CODIGO_MAP[key]
    try:
        return int(tipo_doc)
    except (ValueError, TypeError):
        return 33


# ── Formato de fecha ────────────────────────────────────────────────────────
def format_fecha_humana(fecha: Any) -> str:
    if not fecha or str(fecha).strip() == "":
        return ""
    s = str(fecha).strip()
    return s if " " in s else f"{s} 00:00:00"


# ── Formateo de un documento individual ─────────────────────────────────────
def format_document_for_node(doc: Dict[str, Any]) -> Dict[str, Any]:
    from app.utils.rut import extract_rut_number, extract_rut_dv

    otros_impuestos: List[Dict[str, Any]] = []
    monto_oi = to_num(doc.get("monto_otro_impuesto", 0))
    codigo_oi = doc.get("codigo_otro_impuesto", "")

    if monto_oi > 0:
        nombre_oi = map_otro_impuesto_nombre(codigo_oi)
        otros_impuestos.append({
            "nombre": nombre_oi,
            "tipo": nombre_oi,
            "monto": monto_oi,
            "porcentaje": None,
            "codigo": str(codigo_oi),
            "descripcion": str(codigo_oi),
        })

    return {
        "numero": str(doc.get("folio", "")),
        "tipo": map_tipo_documento(doc.get("tipo_doc", "")),
        "codigo": map_codigo_tipo_documento(doc.get("tipo_doc", "")),
        "fecha_humana": format_fecha_humana(doc.get("fecha")),
        "fecha_humana_emision": format_fecha_humana(doc.get("fecha")),
        "recepcion": {
            "fecha_recepcion_humana": format_fecha_humana(doc.get("fecha_recepcion")),
            "fecha_acuse_humana": format_fecha_humana(doc.get("fecha_acuse")),
            "estado": "",
        },
        "emisor": {
            "rut": extract_rut_number(doc.get("rut_proveedor", "")),
            "dv": extract_rut_dv(doc.get("rut_proveedor", "")),
            "razon_social": str(doc.get("razon_social", "")),
        },
        "receptor": {"razon_social": ""},
        "total": {
            "neto": to_num(doc.get("monto_neto", 0)),
            "impuesto": to_num(doc.get("monto_iva_recuperable", 0)),
            "retencion": to_num(doc.get("monto_retencion", 0)),
            "exento": to_num(doc.get("monto_exento", 0)),
            "total": to_num(doc.get("monto_total", 0)),
            "detalles_otros_impuestos": otros_impuestos,
        },
        "descripcion": [],
        "pdf": None,
        "xml": None,
        "status": "ok",
        "pagado": False,
    }


# ── Formateo de listas completas ────────────────────────────────────────────
def format_documents_for_node(documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [format_document_for_node(doc) for doc in documents]


def build_sii_response(
    compras: List[Dict[str, Any]],
    boletas: List[Dict[str, Any]],
    extra_metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    compras_fmt = format_documents_for_node(compras)
    boletas_fmt = format_documents_for_node(boletas)
    all_docs = compras_fmt + boletas_fmt

    metadata: Dict[str, Any] = {
        "compras_count": len(compras),
        "boletas_count": len(boletas),
        "compras_formateadas": len(compras_fmt),
        "boletas_formateadas": len(boletas_fmt),
    }
    if extra_metadata:
        metadata.update(extra_metadata)

    return {
        "status": "ok",
        "documentos": all_docs,
        "total": len(all_docs),
        "metadata": metadata,
    }
