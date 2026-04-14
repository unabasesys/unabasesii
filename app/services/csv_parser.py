"""
Parser de CSVs generados por los scrapers del SII.

Separado en su propio modulo para testeo y reutilizacion.
"""

import csv
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Encodings a probar en orden
_ENCODINGS = ("utf-8-sig", "utf-8", "latin-1", "windows-1252")


def _open_with_fallback(path: Path):
    """Abre un archivo probando multiples encodings."""
    for enc in _ENCODINGS:
        try:
            fh = open(path, "r", encoding=enc)
            fh.readline()  # test read
            fh.seek(0)
            logger.debug("[CSV-PARSER] Abierto con encoding: %s", enc)
            return fh
        except UnicodeDecodeError:
            try:
                fh.close()
            except Exception:
                pass
    raise RuntimeError(f"No se pudo leer {path} con ninguno de los encodings: {_ENCODINGS}")


def _detect_delimiter(first_line: str) -> str:
    if ";" in first_line:
        return ";"
    if "," in first_line:
        return ","
    return "\t"


# ── Compras ─────────────────────────────────────────────────────────────────
def parse_compras_row(row: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        return {
            "folio": str(row.get("Folio", "")),
            "tipo_doc": str(row.get("Tipo Doc", "")),
            "rut_proveedor": str(row.get("RUT Proveedor", "")),
            "razon_social": str(row.get("Razon Social", "")),
            "fecha": str(row.get("Fecha Docto", "")),
            "fecha_recepcion": str(row.get("Fecha Recepcion", "")),
            "fecha_acuse": str(row.get("Fecha Acuse", "")),
            "monto_neto": str(row.get("Monto Neto", "0")),
            "monto_exento": str(row.get("Monto Exento", "0")),
            "monto_iva_recuperable": str(row.get("Monto IVA Recuperable", "0")),
            "monto_otro_impuesto": str(row.get("Valor Otro Impuesto", "0")),
            "codigo_otro_impuesto": str(row.get("Codigo Otro Impuesto", "0")),
            "monto_total": str(row.get("Monto Total", "0")),
        }
    except Exception as exc:
        logger.warning("[CSV-PARSER] Error parseando fila compras: %s", exc)
        return None


# ── Boletas ─────────────────────────────────────────────────────────────────
def _find_razon_social(row: Dict[str, str]) -> str:
    """Busca razon social en variantes de headers."""
    for key in row:
        if any(k in key for k in ("Nombre", "Razón", "razon", "Razon")):
            val = str(row[key]).strip()
            if val:
                return val
    # Fallback directo
    for candidate in ("Nombre o Razón Social", "Nombre o Razón  Social"):
        val = str(row.get(candidate, "")).strip()
        if val:
            return val
    return ""


def _get_boleta_field(row: Dict[str, str], *keys: str) -> str:
    """Busca el primer key no vacío (tolerante a variaciones de nombre)."""
    for k in keys:
        val = row.get(k)
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


def parse_boletas_row(row: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        folio = _get_boleta_field(row, "N°", "Nº", "N", "Numero", "Número")
        if not folio or "Totales" in str(row):
            return None

        fecha_boleta = _get_boleta_field(row, "Fecha")
        estado = _get_boleta_field(row, "Estado")
        fecha_anulacion = _get_boleta_field(row, "Fecha Anulación", "Fecha Anulacion")

        return {
            "folio": folio,
            "tipo_doc": "boleta_honorarios",
            "rut_proveedor": _get_boleta_field(row, "Rut", "RUT"),
            "razon_social": _find_razon_social(row),
            # La planilla mensual del SII trae la fecha de emisión de la
            # boleta. No existe un "fecha de recepción" separado, así que
            # reutilizamos la fecha de emisión para que Node muestre algo
            # coherente en recepcion.fecha_recepcion_humana.
            "fecha": fecha_boleta,
            "fecha_recepcion": fecha_boleta,
            # Solo poblar fecha_acuse si la boleta está anulada (ahí hay fecha real).
            "fecha_acuse": fecha_anulacion,
            "estado": estado,
            "monto_neto": _get_boleta_field(row, "Brutos") or "0",
            "monto_exento": "0",
            "monto_retencion": _get_boleta_field(row, "Retenido") or "0",
            "monto_total": _get_boleta_field(row, "Pagado") or "0",
        }
    except Exception as exc:
        logger.warning("[CSV-PARSER] Error parseando fila boletas: %s", exc)
        return None


# ── Funcion principal ───────────────────────────────────────────────────────
def parse_csv(csv_path: Path, tipo: str) -> List[Dict[str, Any]]:
    """
    Parsea CSV de compras o boletas y retorna lista de documentos.

    Args:
        csv_path: Ruta al archivo CSV
        tipo: "compras" o "boletas"
    """
    if not csv_path.exists():
        logger.warning("[CSV-PARSER] CSV no existe: %s", csv_path)
        return []

    row_parser = parse_compras_row if tipo == "compras" else parse_boletas_row
    documentos: List[Dict[str, Any]] = []

    try:
        fh = _open_with_fallback(csv_path)
    except RuntimeError as exc:
        logger.error("[CSV-PARSER] %s", exc)
        return []

    try:
        if tipo == "boletas":
            # Boletas tienen 3 líneas de prefijo (info_line, grupos_line, header_line).
            # IMPORTANTE: detectar delimitador desde la LÍNEA DE HEADERS, no desde
            # el info_line que es texto libre del título y puede contener cualquier
            # carácter (';', ':', etc.) y confundir al detector.
            info_line = fh.readline()  # info
            grupos_line = fh.readline()  # grupos
            header_line = fh.readline()
            delimiter = _detect_delimiter(header_line)
            try:
                headers = [h.strip() for h in next(csv.reader([header_line], delimiter=delimiter))]
            except Exception:
                headers = [h.strip().strip('"') for h in header_line.strip().split(delimiter)]
            logger.info("[CSV-PARSER] Headers boletas: %s", headers)
            reader = csv.DictReader(fh, delimiter=delimiter, fieldnames=headers)
        else:
            first_line = fh.readline()
            fh.seek(0)
            delimiter = _detect_delimiter(first_line)
            reader = csv.DictReader(fh, delimiter=delimiter)

        for row in reader:
            doc = row_parser(row)
            if doc:
                documentos.append(doc)

    except Exception as exc:
        logger.error("[CSV-PARSER] Error parseando CSV %s: %s", csv_path, exc)
    finally:
        fh.close()

    logger.info("[CSV-PARSER] Parseados %d documentos de %s", len(documentos), tipo)
    return documentos
