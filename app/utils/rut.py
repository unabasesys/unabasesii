"""
Utilidades para formatear y validar RUT chileno.
"""

import re


def formatear_rut(rut_raw: str) -> str:
    """Asegura formato XXXXXXXX-X (sin puntos, con guion)."""
    rut = rut_raw.replace(".", "").strip()
    if "-" not in rut and len(rut) > 1:
        rut = rut[:-1] + "-" + rut[-1]
    return rut


def rut_completo(rut: str, dv: str) -> str:
    """Combina rut + dv en formato 'XXXXXXXX-X'."""
    return f"{rut.strip()}-{dv.strip()}"


def extract_rut_number(rut_str: str) -> str:
    """Extrae solo el numero (sin DV) de un RUT con cualquier formato."""
    if not rut_str:
        return ""
    clean = str(rut_str).replace(".", "").strip()
    if "-" in clean:
        return clean.split("-")[0]
    # Sin guion: quitar ultimo caracter si es letra
    if clean and not clean[-1].isdigit():
        return clean[:-1]
    if len(clean) > 8:
        return clean[:-1]
    return clean


def extract_rut_dv(rut_str: str) -> str:
    """Extrae el digito verificador de un RUT."""
    if not rut_str:
        return ""
    clean = str(rut_str).replace(".", "").replace(" ", "").strip()
    if "-" in clean:
        parts = clean.split("-")
        return parts[1].upper() if len(parts) > 1 else ""
    if clean and not clean[-1].isdigit():
        return clean[-1].upper()
    return ""


def normalize_rut_for_compare(value: str) -> str:
    """Normaliza un RUT para comparacion (solo alfanumericos uppercase)."""
    return re.sub(r"[^0-9A-Z]", "", (value or "").upper())
