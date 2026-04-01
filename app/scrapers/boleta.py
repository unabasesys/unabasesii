# boletas_exportador_pruebas_ultra_args.py
# -*- coding: utf-8 -*-
import re
import sys
import time
import json
import csv
import html
import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import unicodedata
from typing import Set, Tuple, List
import requests
from datetime import datetime

import pandas as pd
from playwright.sync_api import (
    Playwright,
    sync_playwright,
    TimeoutError as PwTimeoutError,
)

from app.scrapers.sii_session import close_sii_session

# ========= Constantes/UI =========
ERR_TXT = "Su requerimiento no ha sido bien recepcionado"
OK_TXT = "Mensual de Boletas de"
PLANILLA_TEXT_RE = re.compile(r"planilla", re.I)
BTN_CONSULTAS_RE = re.compile(r"Consultas sobre boletas", re.I)
LINK_RECIBIDAS_RE = re.compile(r"Consultar boletas recibidas", re.I)
ROW_MENSUAL_RE = re.compile(r"Mensual de Boletas de", re.I)
PAGINA_SIGUIENTE_RE = re.compile(r"p[aá]gina\s+siguiente", re.I)
PAGINA_INFO_RE = re.compile(r"p[aá]gina\s+(\d+)\s+de\s+(\d+)", re.I)

# CSS selector directo para el boton Planilla (sin get_by_role)
PLANILLA_CSS_SELECTOR = (
    'input[type="button"][value*="lanilla" i], '
    'input[type="submit"][value*="lanilla" i], '
    'button:has-text("lanilla"), '
    'a:has-text("lanilla")'
)

# ========= Reintentos / backoff =========
REINTENTOS_ERROR_SII_DEF = 3
REINTENTOS_CLICK_DEF = 3
BACKOFF_BASE_MS_DEF = 700
VISTA_MENSUAL_TIMEOUT_MS_DEF = 30000
PLANILLA_DOWNLOAD_TIMEOUT_MS_DEF = 30000
PLANILLA_REINTENTOS_DEF = 2



SAVE_PDF_URL = "https://frank.unabase.com/node/savePdfBoleta"

BOLETA_PDF_CACHE_DIR = Path("downloads") / "_cache"
BOLETA_PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _boleta_cache_path(hostname: str) -> Path:
    safe_host = re.sub(r"[^\w\-]", "_", (hostname or "sin_hostname").strip()) or "sin_hostname"
    return BOLETA_PDF_CACHE_DIR / f"boletas_descargadas_{safe_host}.json"


def load_boleta_pdf_cache(hostname: str) -> Set[str]:
    path = _boleta_cache_path(hostname)
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return set(data) if isinstance(data, list) else set()
    except Exception as exc:
        log(f"No se pudo leer cache de boletas descargadas {path}: {exc}")
        return set()


def save_boleta_pdf_cache(hostname: str, cache: Set[str]) -> None:
    path = _boleta_cache_path(hostname)
    try:
        path.write_text(json.dumps(sorted(cache), ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        log(f"No se pudo guardar cache de boletas descargadas {path}: {exc}")


def enviar_pdf_a_api(download, nombre_archivo: str, hostname: Optional[str], session: requests.Session) -> bool:
    try:
        temp_path = download.path()
        if not temp_path:
            log(f"No se pudo obtener path temporal para {nombre_archivo}")
            return False

        with open(temp_path, "rb") as f:
            files = {
                "file": (nombre_archivo, f, "application/pdf")
            }
            data = {
                "filename": nombre_archivo,
                "hostname": hostname or ""
            }

            resp = session.post(
                SAVE_PDF_URL,
                files=files,
                data=data,
                timeout=(10, 60)
            )

        if 200 <= resp.status_code < 300:
            return True

        log(f"API respondió {resp.status_code} al guardar {nombre_archivo}: {resp.text[:300]}")
        return False

    except Exception as e:
        log(f"Error enviando PDF a API ({nombre_archivo}): {e}")
        return False


@dataclass
class ResultDescarga:
    modo_conversion: Optional[str] = None
    archivo_xls: Optional[Path] = None
    archivo_csv: Optional[Path] = None
    paginas_procesadas: int = 0
    pdfs_descargados: int = 0


def log(msg: str) -> None:
    print(f"[BOLETAS] {msg}")


def backoff_sleep(intento: int, base_ms: int) -> None:
    t = (base_ms * (2 ** (intento - 1))) / 1000.0
    time.sleep(min(t, 8))  # cap de 8s


def dump_diagnostico(page, diag_dir: Path, nombre: str) -> None:
    try:
        diag_dir.mkdir(parents=True, exist_ok=True)
        html_path = diag_dir / f"{nombre}.html"
        png_path = diag_dir / f"{nombre}.png"
        html = page.content()
        html_path.write_text(html, encoding="utf-8", errors="ignore")
        page.screenshot(path=str(png_path), full_page=True)
        log(f"Diagnóstico guardado: {html_path} / {png_path}")
    except Exception as e:
        log(f"Diagnóstico falló: {e}")


def esperar_post_login(page, timeout_ms=24000) -> str:
    start = time.time()
    poll = 0.25
    while (time.time() - start) * 1000 < timeout_ms:
        try:
            if page.locator(f"text={ERR_TXT}").first.is_visible():
                return "error"
        except Exception:
            pass
        try:
            if page.get_by_text(OK_TXT, exact=False).first.is_visible():
                return "ok"
        except Exception:
            pass
        try:
            if page.get_by_role("button", name=PLANILLA_TEXT_RE).first.is_visible():
                return "ok"
        except Exception:
            pass
        time.sleep(poll)
    return "timeout"


def safe_click(locator, descripcion: str, retries: int, backoff_base_ms: int) -> None:
    for intento in range(1, retries + 1):
        try:
            locator.scroll_into_view_if_needed()
            locator.click(timeout=6000)
            return
        except Exception:
            if intento == retries:
                raise
            time.sleep(0.3)
            try:
                locator.evaluate("el => el && el.blur && el.blur()")
            except Exception:
                pass
            backoff_sleep(intento, backoff_base_ms)
    raise RuntimeError(f"No se pudo hacer click en {descripcion}")


def obtener_locator_planilla(page, frame=None):
    target = frame or page
    candidatos = [
        ("role_button", target.get_by_role("button", name=PLANILLA_TEXT_RE)),
        ("button_has_text", target.locator('button:has-text("Planilla")')),
        ("input_button", target.locator('input[type="button"][value*="Planilla" i], input[type="submit"][value*="Planilla" i]')),
        ("a_has_text", target.locator('a:has-text("Planilla")')),
    ]

    for nombre, locator in candidatos:
        try:
            c = locator.count()
            if c > 0:
                return locator.first
        except Exception:
            pass

    return candidatos[0][1].first


def obtener_locator_pagina_siguiente(page):
    candidatos = [
        page.get_by_role("button", name=PAGINA_SIGUIENTE_RE),
        page.locator('input[type="button"][value*="Siguiente" i], input[type="submit"][value*="Siguiente" i]'),
        page.locator('input[name="opcion3"]'),
        page.locator('a:has-text("Siguiente"), a:has-text("siguiente")'),
        page.locator('button:has-text("Siguiente"), button:has-text("siguiente")'),
        page.locator('[onclick*="siguiente" i]'),
    ]

    for locator in candidatos:
        try:
            if locator.count() > 0:
                first = locator.first
                # Verificar que sea visible y habilitado
                try:
                    if first.is_visible() and first.is_enabled():
                        return first
                except Exception:
                    return first
        except Exception:
            pass

    return candidatos[0].first


def obtener_info_paginacion_mensual(page) -> Tuple[Optional[int], Optional[int]]:
    actual = None
    total = None

    # Estrategia 1: input hidden pagina_solicitada
    try:
        pagina_solicitada = page.locator('input[name="pagina_solicitada"]').first
        if pagina_solicitada.count() > 0:
            valor = (pagina_solicitada.input_value() or "").strip()
            if valor.isdigit():
                actual = int(valor)
    except Exception:
        pass

    # Estrategia 2: input hidden pagina_actual
    if actual is None:
        try:
            pagina_actual = page.locator('input[name="pagina_actual"]').first
            if pagina_actual.count() > 0:
                valor = (pagina_actual.input_value() or "").strip()
                if valor.isdigit():
                    actual = int(valor)
        except Exception:
            pass

    # Estrategia 3: texto "página X de Y" en la pagina
    try:
        texto = page.get_by_text(PAGINA_INFO_RE, exact=False).first.inner_text()
        match = PAGINA_INFO_RE.search(texto or "")
        if match:
            actual = actual or int(match.group(1))
            total = int(match.group(2))
    except Exception:
        pass

    # Estrategia 4: buscar en todo el body con JS si las anteriores fallaron
    if actual is None or total is None:
        try:
            info = page.evaluate("""() => {
                const body = document.body.innerText || '';
                // Buscar "página X de Y" o "Pagina X de Y"
                const m = body.match(/[Pp][aá]gina\\s+(\\d+)\\s+de\\s+(\\d+)/);
                if (m) return { actual: parseInt(m[1]), total: parseInt(m[2]) };
                // Buscar "Pag. X de Y" o "Pag X/Y"
                const m2 = body.match(/[Pp]ag\\.?\\s*(\\d+)\\s*(?:de|\\/)\\s*(\\d+)/);
                if (m2) return { actual: parseInt(m2[1]), total: parseInt(m2[2]) };
                return null;
            }""")
            if info:
                actual = actual or info.get("actual")
                total = total or info.get("total")
        except Exception:
            pass

    return actual, total


def describir_estado_mensual(page) -> str:
    partes = [f"url={page.url}"]

    try:
        pagina_actual, pagina_total = obtener_info_paginacion_mensual(page)
        if pagina_actual is not None:
            partes.append(f"pagina_actual={pagina_actual}")
        if pagina_total is not None:
            partes.append(f"pagina_total={pagina_total}")
    except Exception:
        partes.append("pagina_actual=?")

    try:
        filas = page.locator("tr.reporte").count()
        partes.append(f"filas_reporte={filas}")
    except Exception:
        partes.append("filas_reporte=?")

    try:
        # Usar CSS directo para diagnostico consistente
        css_loc = page.locator(PLANILLA_CSS_SELECTOR)
        css_count = css_loc.count()
        partes.append(f"planilla_css_count={css_count}")
        if css_count > 0:
            visible = css_loc.first.is_visible()
            enabled = css_loc.first.is_enabled() if visible else False
            partes.append(f"planilla_visible={visible}")
            partes.append(f"planilla_enabled={enabled}")
        else:
            # Fallback a obtener_locator_planilla
            planilla = obtener_locator_planilla(page)
            visible = planilla.is_visible()
            enabled = planilla.is_enabled() if visible else False
            partes.append(f"planilla_visible={visible}")
            partes.append(f"planilla_enabled={enabled}")
    except Exception as e:
        partes.append(f"planilla_visible=False")
        partes.append(f"planilla_enabled=False")
        partes.append(f"planilla_error={type(e).__name__}")

    return ", ".join(partes)


def obtener_clave_primera_fila_mensual(page) -> str:
    selector_filas = (
        'tr.reporte:has(a[href*="ObtenerBoletaPdf"]), '
        'tr.reporte:has(a[onclick*="ObtenerBoletaPdf"])'
    )

    try:
        fila = page.locator(selector_filas).first
        if fila.count() == 0:
            return ""

        columnas = fila.locator("td")
        folio = _safe_text(columnas.nth(1))
        fecha = _safe_text(columnas.nth(3))
        rut = _safe_text(columnas.nth(4))
        return "|".join([folio, fecha, rut]).strip("|")
    except Exception:
        return ""


def _buscar_planilla_en_frames(page):
    """Busca el boton Planilla en todos los frames de la pagina."""
    for frame in page.frames:
        try:
            planilla = obtener_locator_planilla(page, frame=frame)
            if planilla.is_visible() and planilla.is_enabled():
                frame_name = frame.name or frame.url
                log(f"Planilla encontrada en frame: {frame_name}")
                return planilla
        except Exception:
            continue
    return None


def esperar_vista_mensual_lista(page, timeout_ms: int = VISTA_MENSUAL_TIMEOUT_MS_DEF):
    start = time.time()
    ultimo_estado = describir_estado_mensual(page)

    # Estrategia 1: wait_for_selector directo con CSS (mas confiable que polling)
    try:
        handle = page.wait_for_selector(
            PLANILLA_CSS_SELECTOR,
            state="visible",
            timeout=min(timeout_ms, 15000),
        )
        if handle:
            log("Planilla detectada via wait_for_selector CSS directo")
            locator = page.locator(PLANILLA_CSS_SELECTOR).first
            if locator.is_enabled():
                return locator
            log("Planilla visible pero no enabled, continuando polling...")
    except Exception as e:
        log(f"wait_for_selector CSS no encontro Planilla: {e}")

    # Estrategia 2: buscar en frames (sitios SII usan framesets)
    try:
        planilla_frame = _buscar_planilla_en_frames(page)
        if planilla_frame:
            return planilla_frame
    except Exception as e:
        log(f"Busqueda en frames fallo: {e}")

    elapsed = (time.time() - start) * 1000
    remaining = timeout_ms - elapsed

    # Estrategia 3: polling con logging detallado
    intentos = 0
    while (time.time() - start) * 1000 < timeout_ms:
        intentos += 1
        try:
            planilla = obtener_locator_planilla(page)
            vis = planilla.is_visible()
            ena = planilla.is_enabled() if vis else False
            if vis and ena:
                log(f"Planilla detectada en polling intento {intentos}")
                return planilla
            if intentos <= 3 or intentos % 20 == 0:
                log(f"Planilla polling #{intentos}: visible={vis}, enabled={ena}")
        except Exception as e:
            if intentos <= 3 or intentos % 20 == 0:
                log(f"Planilla polling #{intentos}: excepcion={type(e).__name__}: {e}")

        ultimo_estado = describir_estado_mensual(page)
        try:
            page.wait_for_load_state("networkidle", timeout=2000)
        except Exception:
            pass
        page.wait_for_timeout(300)

    raise RuntimeError(
        "La vista mensual no quedo lista para descargar la planilla. "
        f"Ultimo estado observado: {ultimo_estado}"
    )


def ir_a_pagina_siguiente_mensual(
    page,
    retries_click: int,
    backoff_base_ms: int,
    timeout_ms: int = VISTA_MENSUAL_TIMEOUT_MS_DEF,
) -> bool:
    btn = obtener_locator_pagina_siguiente(page)
    try:
        if btn.count() == 0 or not btn.is_visible() or not btn.is_enabled():
            return False
    except Exception:
        return False

    pagina_antes, total_paginas = obtener_info_paginacion_mensual(page)
    primera_fila_antes = obtener_clave_primera_fila_mensual(page)

    # Si sabemos que estamos en la ultima pagina, no intentar avanzar
    if pagina_antes is not None and total_paginas is not None and pagina_antes >= total_paginas:
        log(f"Ya estamos en la ultima pagina ({pagina_antes}/{total_paginas})")
        return False

    log(
        "Avanzando pagina mensual: "
        f"actual={pagina_antes or '?'} total={total_paginas or '?'}"
    )

    click_con_fallback(btn, "Pagina Siguiente", retries_click, backoff_base_ms)

    start = time.time()
    ultimo_estado = describir_estado_mensual(page)

    while (time.time() - start) * 1000 < timeout_ms:
        try:
            page.wait_for_load_state("networkidle", timeout=1200)
        except Exception:
            pass

        try:
            planilla = esperar_vista_mensual_lista(page, timeout_ms=2000)
            planilla_lista = planilla.is_visible() and planilla.is_enabled()
        except Exception:
            planilla_lista = False

        pagina_despues, total_despues = obtener_info_paginacion_mensual(page)
        primera_fila_despues = obtener_clave_primera_fila_mensual(page)
        ultimo_estado = describir_estado_mensual(page)

        cambio_pagina = (
            pagina_antes is not None
            and pagina_despues is not None
            and pagina_despues != pagina_antes
        )
        cambio_fila = bool(
            primera_fila_antes
            and primera_fila_despues
            and primera_fila_despues != primera_fila_antes
        )

        # Si la planilla esta lista y detectamos cambio de pagina o fila
        if planilla_lista and (cambio_pagina or cambio_fila):
            log(
                "Pagina mensual lista: "
                f"actual={pagina_despues or '?'} total={total_despues or '?'}"
            )
            return True

        # Si la planilla esta lista pero no pudimos detectar cambio (info de paginacion no disponible),
        # aceptar el avance si paso suficiente tiempo para que la pagina se cargue
        if planilla_lista and pagina_antes is None and primera_fila_antes == "":
            elapsed_ms = (time.time() - start) * 1000
            if elapsed_ms > 2000:
                log(
                    "Pagina mensual lista (sin info de paginacion previa para comparar): "
                    f"actual={pagina_despues or '?'} total={total_despues or '?'}"
                )
                return True

        page.wait_for_timeout(300)

    # En vez de lanzar excepcion, retornar False para que el loop de paginacion
    # se detenga graciosamente en lugar de abortar toda la operacion
    log(
        "No se pudo confirmar avance a pagina siguiente. "
        f"Ultimo estado observado: {ultimo_estado}"
    )
    return False


def click_con_fallback(locator, descripcion: str, retries: int, backoff_base_ms: int) -> None:
    try:
        safe_click(locator, descripcion, retries, backoff_base_ms)
        return
    except Exception:
        try:
            locator.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            locator.evaluate("el => el.click()")
            return
        except Exception as js_error:
            raise RuntimeError(
                f"No se pudo hacer click en {descripcion} ni con fallback JS"
            ) from js_error


def esperar_y_disparar_descarga_planilla(
    page,
    retries_click: int,
    backoff_base_ms: int,
    diag_dir: Path,
    timeout_ms: int = PLANILLA_DOWNLOAD_TIMEOUT_MS_DEF,
    reintentos: int = PLANILLA_REINTENTOS_DEF,
):
    ultimo_error = None

    for intento in range(1, reintentos + 1):
        planilla = esperar_vista_mensual_lista(page)
        estado = describir_estado_mensual(page)
        log(f"Planilla lista. Intento {intento}/{reintentos}. Estado: {estado}")

        try:
            with page.expect_download(timeout=timeout_ms) as dl:
                click_con_fallback(planilla, "Boton Planilla", retries_click, backoff_base_ms)
            return dl.value
        except Exception as e:
            ultimo_error = e
            log(f"No se inicio la descarga de planilla en el intento {intento}: {e}")
            dump_diagnostico(page, diag_dir, f"planilla_timeout_intento_{intento}")

            if intento < reintentos:
                try:
                    page.wait_for_load_state("networkidle", timeout=2000)
                except Exception:
                    pass
                page.wait_for_timeout(1200)

    raise RuntimeError(
        f"No se pudo iniciar la descarga de Planilla tras {reintentos} intentos: {ultimo_error}"
    )


def forzar_formato_rut(page, rut_sin_formato: str, dv: Optional[str]) -> None:
    """
    Soporta login de una o dos cajas:
    - Si hay una sola caja (placeholder 'Ej:'), escribe rut completo (con o sin guion; el sitio formatea).
    - Si detecta cajas separadas para RUT y DV, llena ambas (usa --dv si viene, si no intenta deducir).
    """
    # Caso 1: único textbox para RUT completo
    unico = None
    try:
        # Buscar por placeholder que contenga "Ej:" o "ejemplo"
        unico = page.get_by_role("textbox", name=re.compile(r"Ej", re.I))
        if unico.count() == 0:
            # También buscar por placeholder vacío o que contenga "RUT"
            unico = page.locator('input[type="text"]').filter(has_not=page.locator('input[placeholder*="Buscar" i]')).first
        else:
            unico = unico.first
    except Exception:
        unico = None

    if unico and unico.is_visible():
        valor = f"{rut_sin_formato}-{dv}" if dv else rut_sin_formato
        log(f"Usando campo único para RUT completo: {valor}")
        # Limpiar y llenar directamente sin formato (el sitio lo formatea automáticamente)
        unico.fill("")
        unico.fill(valor)
        page.wait_for_timeout(500)
        log(f"RUT ingresado en campo único")
        return

    # Caso 2: cajas separadas (RUT + DV): heurísticas comunes
    rut_box = None
    dv_box = None
    
    # Buscamos por name o id típicos primero
    try:
        rut_box = page.locator('input[name="rut"], input[id="rut"], input[placeholder*="RUT" i]').first
        dv_box = page.locator('input[name="dv"], input[id="dv"], input[placeholder*="DV" i]').first
    except Exception:
        pass
    
    # Si no encontramos los campos específicos, buscar TODOS los inputs de texto
    if rut_box is None or rut_box.count() == 0 or dv_box.count() == 0:
        log("No se encontraron campos por nombre/ID, buscando todos los inputs...")
        try:
            # Obtener TODOS los inputs de texto de la página
            all_inputs = page.locator('input[type="text"]').all()
            log(f"Total inputs de texto encontrados: {len(all_inputs)}")
            
            visible_inputs = []
            for idx, input_elem in enumerate(all_inputs):
                try:
                    # Verificar visibilidad
                    is_visible = input_elem.is_visible()
                    placeholder = input_elem.get_attribute('placeholder') or ""
                    input_id = input_elem.get_attribute('id') or ""
                    input_name = input_elem.get_attribute('name') or ""
                    
                    log(f"Input {idx}: visible={is_visible}, placeholder='{placeholder}', id='{input_id}', name='{input_name}'")
                    
                    if is_visible and 'Buscar' not in placeholder.lower():
                        visible_inputs.append(input_elem)
                except Exception as e:
                    log(f"Error verificando input {idx}: {e}")
                    continue
            
            log(f"Encontrados {len(visible_inputs)} inputs visibles (sin 'Buscar')")
            
            # Usar los primeros dos inputs visibles (que no sean de búsqueda)
            if len(visible_inputs) >= 2:
                rut_box = visible_inputs[0]
                dv_box = visible_inputs[1]
                log(f"Usando inputs: placeholder[0]='{rut_box.get_attribute('placeholder')}', placeholder[1]='{dv_box.get_attribute('placeholder')}'")
            else:
                log("Error: No se encontraron suficientes campos de input visibles para login")
                raise Exception("No se pudieron encontrar los campos RUT y DV del formulario de login")
        except Exception as e:
            log(f"No se encontraron campos RUT/DV adecuados: {e}")
            raise

    # Preparar RUT y DV
    rut_base = rut_sin_formato
    dv_final = (dv or "").strip()
    if not dv_final:
        # Si no nos dieron dv y el rut trae más de 1 dígito, intentamos tomar el último como dv.
        if len(rut_base) > 1:
            rut_base, dv_final = rut_base[:-1], rut_base[-1]

    # Llenar campos RUT y DV
    log(f"Llenando RUT: {rut_base}, DV: {dv_final}")
    rut_box.fill("")
    rut_box.fill(rut_base)
    page.wait_for_timeout(200)
    dv_box.fill("")
    dv_box.fill(dv_final)
    page.wait_for_timeout(500)
    log("Campos RUT y DV llenados")


def convertir_a_csv(xls_path: Path, csv_path: Path) -> str:
    header = b""
    with open(xls_path, "rb") as f:
        header = f.read(4096)

    def _norm_write(df: pd.DataFrame):
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        df = df.dropna(how="all")
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    is_html = header.strip().lower().startswith(b"<") or b"<html" in header.lower()
    if is_html:
        tables = pd.read_html(xls_path, header=0)  # requiere lxml
        if not tables:
            raise RuntimeError("No se encontraron tablas HTML en la planilla.")
        best = max(tables, key=lambda d: int(d.shape[0]) * int(d.shape[1]))
        _norm_write(best)
        return "html"

    try:
        df = pd.read_excel(xls_path, engine="xlrd")  # .xls clásico
        _norm_write(df)
        return "excel-xlrd"
    except Exception:
        tables = pd.read_html(xls_path, header=0)
        if not tables:
            raise
        best = max(tables, key=lambda d: int(d.shape[0]) * int(d.shape[1]))
        _norm_write(best)
        return "fallback-html"

# --- helpers filename seguros ---
def _sanitize_filename(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^\w\-. ]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"\s+", "_", s).strip("_")
    return s or "boleta"

def _safe_text(loc):
    try:
        return loc.inner_text().strip()
    except Exception:
        return ""

def _rut_para_nombre(rut_txt: str) -> str:
    """
    Normaliza el RUT para usar en el nombre de archivo:
    - quita puntos, espacios y guion
    - mantiene dígitos y K/k (pasa a mayúscula)
    """
    s = (rut_txt or "").upper()
    s = re.sub(r'[^0-9K]', '', s)  # deja solo 0-9 y K
    return s


def _normalizar_texto_clave(texto: str) -> str:
    s = (texto or "").replace("\xa0", " ").strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s)
    return s.lower()


def _indice_columna_boletas(headers: List[str], nombre_esperado: str) -> Optional[int]:
    esperado = _normalizar_texto_clave(nombre_esperado)
    for idx, header in enumerate(headers):
        if _normalizar_texto_clave(header) == esperado:
            return idx
    return None


def _estado_anulado(estado: str) -> bool:
    normalizado = _normalizar_texto_clave(estado)
    return "anul" in normalizado or normalizado.startswith("nul")


def _a_entero_boleta(valor: str) -> int:
    limpio = re.sub(r"[^\d\-]", "", str(valor or ""))
    if not limpio or limpio == "-":
        return 0
    try:
        return int(limpio)
    except Exception:
        return 0


def _leer_fragmento_csv_boletas(csv_path: Path):
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        lineas = f.read().splitlines()

    if len(lineas) < 3:
        raise RuntimeError(f"CSV de boletas incompleto: {csv_path}")

    info_line = lineas[0]
    grupos_line = lineas[1]
    header_line = lineas[2]
    headers = [h.strip() for h in next(csv.reader([header_line]))]

    filas = []
    for line in lineas[3:]:
        if not line.strip():
            continue
        row = [c.strip() for c in next(csv.reader([line]))]
        if row and row[0].startswith("Totales*"):
            continue
        filas.append(row)

    return info_line, grupos_line, header_line, headers, filas


def _construir_fila_totales_boletas(headers: List[str], filas: List[List[str]]) -> List[str]:
    fila_totales = ["Totales* :"] * len(headers)

    idx_estado = _indice_columna_boletas(headers, "Estado")
    idx_brutos = _indice_columna_boletas(headers, "Brutos")
    idx_retenido = _indice_columna_boletas(headers, "Retenido")
    idx_pagado = _indice_columna_boletas(headers, "Pagado")

    total_brutos = 0
    total_retenido = 0
    total_pagado = 0

    for fila in filas:
        if idx_estado is not None and idx_estado < len(fila) and _estado_anulado(fila[idx_estado]):
            continue
        if idx_brutos is not None and idx_brutos < len(fila):
            total_brutos += _a_entero_boleta(fila[idx_brutos])
        if idx_retenido is not None and idx_retenido < len(fila):
            total_retenido += _a_entero_boleta(fila[idx_retenido])
        if idx_pagado is not None and idx_pagado < len(fila):
            total_pagado += _a_entero_boleta(fila[idx_pagado])

    if idx_brutos is not None:
        fila_totales[idx_brutos] = str(total_brutos)
    if idx_retenido is not None:
        fila_totales[idx_retenido] = str(total_retenido)
    if idx_pagado is not None:
        fila_totales[idx_pagado] = str(total_pagado)

    return fila_totales


def _escribir_csv_boletas_combinado(
    out_csv: Path,
    info_line: str,
    grupos_line: str,
    header_line: str,
    filas: List[List[str]],
    fila_totales: List[str],
) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    try:
        out_csv.unlink(missing_ok=True)
    except Exception:
        pass

    with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
        f.write(info_line.rstrip("\n") + "\n")
        f.write(grupos_line.rstrip("\n") + "\n")
        f.write(header_line.rstrip("\n") + "\n")
        writer = csv.writer(f, lineterminator="\n")
        writer.writerows(filas)
        writer.writerow(fila_totales)


def _escribir_xls_boletas_combinado(
    out_xls: Path,
    info_line: str,
    headers: List[str],
    filas: List[List[str]],
    fila_totales: List[str],
) -> None:
    out_xls.parent.mkdir(parents=True, exist_ok=True)
    try:
        out_xls.unlink(missing_ok=True)
    except Exception:
        pass

    headers_limpios = [h.replace("\xa0", " ").strip() for h in headers]
    filas_exportacion = filas + [fila_totales]
    df = pd.DataFrame(filas_exportacion, columns=headers_limpios)
    titulo = html.escape((info_line.split(",")[0] if info_line else "Informe mensual de boletas"))
    tabla = df.to_html(index=False, border=1)
    contenido = (
        "<html><head><meta charset=\"utf-8\"></head><body>"
        f"<p>{titulo}</p>{tabla}</body></html>"
    )
    out_xls.write_text(contenido, encoding="utf-8")

def _descargar_pdfs_boletas_pagina(
    page,
    carpeta_destino: Path,
    max_descargas: Optional[int] = None,
    session: Optional[requests.Session] = None,
    hostname: Optional[str] = None,
    guardar_local: bool = False,
    downloaded_cache: Optional[Set[str]] = None,
) -> int:
    carpeta_destino.mkdir(parents=True, exist_ok=True)

    try:
        selector_filas = (
            'tr.reporte:has(a[href*="ObtenerBoletaPdf"]), '
            'tr.reporte:has(a[onclick*="ObtenerBoletaPdf"])'
        )
        page.wait_for_selector(selector_filas, timeout=10000)
    except Exception:
        log("No se encontraron filas de boletas con link PDF")
        return 0

    filas = page.locator(selector_filas)
    total = filas.count()
    log(f"Filas encontradas (con PDF): {total}")

    descargados = 0
    for i in range(total):
        if max_descargas is not None and descargados >= max_descargas:
            break

        fila = filas.nth(i)
        link_pdf = fila.locator('a[href*="ObtenerBoletaPdf"], a[onclick*="ObtenerBoletaPdf"]').first

        try:
            if link_pdf.count() == 0:
                log(f"Fila {i+1}: sin link PDF → salto.")
                continue

            columnas = fila.locator("td")
            folio = _safe_text(columnas.nth(1))
            estado = _safe_text(columnas.nth(2).locator("a").first) or _safe_text(columnas.nth(2))
            rut_txt = _safe_text(columnas.nth(4))
            rut_norm = _rut_para_nombre(rut_txt)

            folio = re.sub(r"\s+", "", folio)
            estado = estado.strip().upper() if estado else "SIN_ESTADO"
            if not rut_norm:
                rut_norm = "SINRUT"

            nombre_archivo = _sanitize_filename(f"boleta_{folio}_{estado}_{rut_norm}.pdf")
            ruta_salida = carpeta_destino / nombre_archivo

            # Verificar en cache si ya fue descargado previamente
            if downloaded_cache is not None and nombre_archivo in downloaded_cache:
                log(f"Cache: ya descargado previamente → salto: {nombre_archivo}")
                continue

            if guardar_local and ruta_salida.exists():
                log(f"Ya existe: {ruta_salida.name} → salto.")
                if downloaded_cache is not None:
                    downloaded_cache.add(nombre_archivo)
                continue

            log(f"Descargando PDF fila {i+1}: {nombre_archivo}")
            with page.expect_download() as dl:
                link_pdf.click()
            download = dl.value

            failure = download.failure()
            if failure:
                log(f"Descarga falló (fila {i+1}): {failure}")
                continue

            exito = False

            if session is not None:
                enviado_ok = enviar_pdf_a_api(download, nombre_archivo, hostname, session)
                if enviado_ok:
                    log(f"PDF enviado correctamente: {nombre_archivo}")
                    exito = True
                else:
                    log(f"No se pudo enviar PDF a API: {nombre_archivo}")

            if guardar_local:
                try:
                    download.save_as(ruta_salida)
                    log(f"PDF guardado localmente: {ruta_salida.name}")
                    exito = True
                except Exception as e:
                    log(f"No se pudo guardar localmente {ruta_salida.name}: {e}")

            if exito:
                descargados += 1
                if downloaded_cache is not None:
                    downloaded_cache.add(nombre_archivo)

        except Exception as e:
            log(f"Error en descarga de PDF (fila {i+1}): {e}")
            continue

    return descargados

# ---------- Navegación / flujo ----------
def descargar_pdfs_boletas(
    page,
    carpeta_destino: Path,
    max_descargas: Optional[int] = None,
    session: Optional[requests.Session] = None,
    hostname: Optional[str] = None,
    guardar_local: bool = False,
    paginar: bool = True,
    retries_click: int = REINTENTOS_CLICK_DEF,
    backoff_base_ms: int = BACKOFF_BASE_MS_DEF,
) -> int:
    total_descargados = 0
    paginas_visitadas = 0

    # Cargar cache de boletas ya descargadas
    cache_key = hostname or "default"
    downloaded_cache = load_boleta_pdf_cache(cache_key)
    log(f"[CACHE] Cache boletas cargada para '{cache_key}': {len(downloaded_cache)} PDFs previamente descargados")

    while True:
        paginas_visitadas += 1
        pagina_actual, total_paginas = obtener_info_paginacion_mensual(page)
        log(
            "Procesando PDFs en pagina mensual "
            f"{pagina_actual or paginas_visitadas}/{total_paginas or '?'}"
        )

        restantes = None
        if max_descargas is not None:
            restantes = max(0, max_descargas - total_descargados)
            if restantes == 0:
                break

        total_descargados += _descargar_pdfs_boletas_pagina(
            page,
            carpeta_destino,
            max_descargas=restantes,
            session=session,
            hostname=hostname,
            guardar_local=guardar_local,
            downloaded_cache=downloaded_cache,
        )

        if not paginar:
            break

        if max_descargas is not None and total_descargados >= max_descargas:
            break

        if not ir_a_pagina_siguiente_mensual(page, retries_click, backoff_base_ms):
            break

    # Guardar cache actualizada
    save_boleta_pdf_cache(cache_key, downloaded_cache)
    log(f"[CACHE] Cache boletas actualizada para '{cache_key}': {len(downloaded_cache)} PDFs totales registrados")

    return total_descargados


def navegar_a_consultar_boletas(page, retries_click: int, backoff_base_ms: int) -> None:
    log("Abriendo portal…")
    page.goto("https://www.sii.cl/servicios_online/1040-1287.html", wait_until="domcontentloaded")

    log("Entrando a 'Consultas sobre boletas'…")
    btn = page.get_by_role("button", name=BTN_CONSULTAS_RE).first
    safe_click(btn, "Consultas sobre boletas", retries_click, backoff_base_ms)

    log("Click en 'Consultar boletas recibidas'…")
    link = page.get_by_role("link", name=LINK_RECIBIDAS_RE).first
    safe_click(link, "Consultar boletas recibidas", retries_click, backoff_base_ms)


def hacer_login(page, rut: str, dv: Optional[str], clave: str,
                retries_click: int, backoff_base_ms: int) -> None:
    log("Autenticando…")
    # Esperar a que la página de login cargue completamente
    log("Esperando que cargue la página de login...")
    try:
        page.wait_for_load_state("networkidle", timeout=10000)
    except:
        pass
    page.wait_for_timeout(500)
    
    forzar_formato_rut(page, rut, dv)
    page.locator("#clave, input[type='password']").first.fill(clave)
    ingresar = page.get_by_role("button", name=re.compile(r"Ingresar$", re.I)).first
    safe_click(ingresar, "Ingresar", retries_click, backoff_base_ms)


def _to_ddmmyyyy(fecha_yyyy_mm_dd: str) -> str:
    # acepta "2025-10-16" o "16/10/2025" y normaliza
    s = fecha_yyyy_mm_dd.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        y, m, d = s.split("-")
        return f"{d}/{m}/{y}"
    if re.match(r"^\d{2}/\d{2}/\d{4}$", s):
        return s
    # último recurso: no tocar
    return s


def intentar_setear_fecha(page, fecha: Optional[str]) -> None:
    """
    Intenta setear una fecha (informe diario) si existen controles de fecha.
    Prueba varios selectores comunes y un botón Buscar/Consultar/Filtrar.
    Silencioso si no encuentra nada (seguirá con mensual).
    """
    if not fecha:
        return
    ddmmyyyy = _to_ddmmyyyy(fecha)

    try:
        # Inputs típicos
        candidatos = [
            'input[name*="fecha" i]',
            'input[id*="fecha" i]',
            'input[placeholder*="fecha" i]',
            'input[name*="desde" i]',
            'input[id*="desde" i]',
            'input[placeholder*="desde" i]',
            'input[name*="dia" i]',
            'input[id*="dia" i]'
        ]
        encontrado = False
        for sel in candidatos:
            loc = page.locator(sel)
            if loc.count() > 0:
                try:
                    loc.first.fill("")
                    loc.first.type(ddmmyyyy, delay=40)
                    encontrado = True
                    break
                except Exception:
                    pass

        if not encontrado:
            return

        # Botones típicos para ejecutar la búsqueda
        btns = [
            page.get_by_role("button", name=re.compile(r"Buscar|Consultar|Filtrar|Actualizar", re.I)).first,
            page.locator('input[type="submit"]').first
        ]
        for b in btns:
            try:
                if b and b.count() > 0 and b.is_visible():
                    b.click(timeout=3000)
                    page.wait_for_timeout(800)
                    break
            except Exception:
                pass
    except Exception:
        pass

def _recuperar_pagina_post_planilla(page, pagina_esperada, total_paginas) -> None:
    """
    Después de descargar la planilla (VerPlanillaMensualRec), el navegador puede
    haber navegado fuera de la lista de boletas.  Esta función detecta ese caso
    y vuelve atrás para restaurar la vista con la paginación intacta.
    """
    page.wait_for_timeout(800)

    # ¿La página sigue mostrando la lista de boletas?
    try:
        check = page.locator(PLANILLA_CSS_SELECTOR)
        if check.count() > 0 and check.first.is_visible():
            log("Página sigue mostrando lista de boletas después de descarga de planilla")
            return
    except Exception:
        pass

    # La página cambió → intentar volver atrás
    log("Página cambió después de descargar planilla, navegando atrás (go_back)...")
    try:
        page.go_back(wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(1000)
    except Exception as e:
        log(f"go_back falló: {e}")

    # Verificar que volvimos a la lista de boletas
    try:
        esperar_vista_mensual_lista(page, timeout_ms=15000)
        log("Página recuperada con go_back")

        # Verificar que estamos en la página correcta
        pag_actual, _ = obtener_info_paginacion_mensual(page)
        if pag_actual is not None and pagina_esperada is not None and pag_actual == pagina_esperada:
            log(f"Confirmado: seguimos en página {pag_actual}")
        elif pag_actual is not None and pagina_esperada is not None:
            log(f"Página actual={pag_actual}, esperada={pagina_esperada} (puede diferir, continuando)")
        return
    except Exception as e:
        log(f"No se pudo recuperar la lista de boletas con go_back: {e}")

    # Último recurso: segundo go_back (a veces el primero llega a una página intermedia)
    log("Intentando segundo go_back...")
    try:
        page.go_back(wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(1000)
        esperar_vista_mensual_lista(page, timeout_ms=15000)
        log("Página recuperada con segundo go_back")
    except Exception as e:
        log(f"Segundo go_back también falló: {e} — la paginación podría no continuar")


def _abrir_mensual_y_descargar_paginado(
    page,
    out_xls: Path,
    out_csv: Path,
    retries_click: int,
    backoff_base_ms: int,
    diag_dir: Path,
    session: Optional[requests.Session] = None,
    hostname: Optional[str] = None,
    pdf_dir: Optional[Path] = None,
    descargar_pdfs: bool = False,
) -> ResultDescarga:
    destino_pdfs = None
    downloaded_cache: Optional[Set[str]] = None
    if descargar_pdfs:
        destino_pdfs = Path(pdf_dir) if pdf_dir else (out_xls.parent / (hostname or "default") / "boletas")
        destino_pdfs.mkdir(parents=True, exist_ok=True)
        log(f"Descargando PDFs a: {destino_pdfs}")

        # Cargar cache de boletas ya descargadas
        cache_key = hostname or "default"
        downloaded_cache = load_boleta_pdf_cache(cache_key)
        log(f"[CACHE] Cache boletas cargada para '{cache_key}': {len(downloaded_cache)} PDFs previamente descargados")

    diag_planillas_dir = diag_dir / "planillas_paginas"
    diag_planillas_dir.mkdir(parents=True, exist_ok=True)

    info_line = None
    grupos_line = None
    header_line = None
    headers = None
    filas_combinadas: List[List[str]] = []
    filas_vistas = set()
    modos_conversion = []
    paginas_procesadas = 0
    pdfs_descargados = 0

    while True:
        pagina_actual, total_paginas = obtener_info_paginacion_mensual(page)
        pagina_log = pagina_actual or (paginas_procesadas + 1)
        log(f"Esperando descarga de 'Planilla' para pagina {pagina_log}/{total_paginas or '?'}...")

        download = esperar_y_disparar_descarga_planilla(
            page,
            retries_click=retries_click,
            backoff_base_ms=backoff_base_ms,
            diag_dir=diag_dir,
        )

        failure = download.failure()
        if failure:
            raise RuntimeError(f"Fallo en descarga: {failure}")

        pagina_tag = f"pagina_{pagina_log}"
        temp_xls = diag_planillas_dir / f"{pagina_tag}.xls"
        temp_csv = diag_planillas_dir / f"{pagina_tag}.csv"

        try:
            temp_xls.unlink(missing_ok=True)
        except Exception:
            pass
        try:
            temp_csv.unlink(missing_ok=True)
        except Exception:
            pass

        download.save_as(temp_xls)
        log(f"Planilla descargada ({download.suggested_filename}) -> {temp_xls.name}")

        modo = convertir_a_csv(temp_xls, temp_csv)
        modos_conversion.append(modo)
        log(f"Planilla pagina {pagina_log} convertida a CSV ({modo})")

        info_tmp, grupos_tmp, header_tmp, headers_tmp, filas_tmp = _leer_fragmento_csv_boletas(temp_csv)
        if info_line is None:
            info_line = info_tmp
            grupos_line = grupos_tmp
            header_line = header_tmp
            headers = headers_tmp

        agregadas = 0
        for fila in filas_tmp:
            fila_key = tuple(fila)
            if fila_key in filas_vistas:
                continue
            filas_vistas.add(fila_key)
            filas_combinadas.append(fila)
            agregadas += 1
        log(f"Pagina {pagina_log}: filas nuevas agregadas={agregadas}")

        # ── Recuperar estado de página después de descargar planilla ──
        # La descarga de planilla (VerPlanillaMensualRec) puede navegar la página
        # causando que se pierda la lista de boletas y los controles de paginación.
        _recuperar_pagina_post_planilla(page, pagina_log, total_paginas)

        if descargar_pdfs and destino_pdfs is not None:
            enviados_pagina = _descargar_pdfs_boletas_pagina(
                page,
                destino_pdfs,
                session=session,
                hostname=hostname,
                guardar_local=False,
                downloaded_cache=downloaded_cache,
            )
            pdfs_descargados += enviados_pagina
            log(f"Pagina {pagina_log}: PDFs procesados={enviados_pagina}")

        paginas_procesadas += 1

        if not ir_a_pagina_siguiente_mensual(page, retries_click, backoff_base_ms):
            break

    # Guardar cache actualizada
    if descargar_pdfs and downloaded_cache is not None:
        cache_key = hostname or "default"
        save_boleta_pdf_cache(cache_key, downloaded_cache)
        log(f"[CACHE] Cache boletas actualizada para '{cache_key}': {len(downloaded_cache)} PDFs totales registrados")

    if info_line is None or grupos_line is None or header_line is None or headers is None:
        raise RuntimeError("No se pudo reconstruir el contenido de la planilla de boletas.")

    fila_totales = _construir_fila_totales_boletas(headers, filas_combinadas)
    _escribir_csv_boletas_combinado(
        out_csv,
        info_line=info_line,
        grupos_line=grupos_line,
        header_line=header_line,
        filas=filas_combinadas,
        fila_totales=fila_totales,
    )
    _escribir_xls_boletas_combinado(
        out_xls,
        info_line=info_line,
        headers=headers,
        filas=filas_combinadas,
        fila_totales=fila_totales,
    )

    modo_final = modos_conversion[0] if len(set(modos_conversion)) == 1 else f"paginado:{'+'.join(sorted(set(modos_conversion)))}"
    log(
        f"Planilla consolidada: paginas={paginas_procesadas}, filas={len(filas_combinadas)}, "
        f"csv={out_csv}, xls={out_xls}"
    )

    return ResultDescarga(
        modo_conversion=modo_final,
        archivo_xls=out_xls,
        archivo_csv=out_csv,
        paginas_procesadas=paginas_procesadas,
        pdfs_descargados=pdfs_descargados,
    )


def abrir_mensual_y_descargar(page, out_xls: Path, out_csv: Path,
                               retries_click: int, backoff_base_ms: int,
                               fecha_objetivo: Optional[str],
                               diag_dir: Path,
                               session: Optional[requests.Session] = None,
                               hostname: Optional[str] = None,
                               pdf_dir: Optional[Path] = None,
                               descargar_pdfs: bool = False) -> ResultDescarga:
    try:
        page.wait_for_selector(f"text={OK_TXT}", timeout=10000)
    except PwTimeoutError:
        pass

    log("Abriendo fila 'Mensual de Boletas de'…")
    fila = page.get_by_role("row", name=ROW_MENSUAL_RE).first
    btn_consultar = fila.locator("#cmdconsultar1")
    safe_click(btn_consultar, "cmdconsultar1 (abrir mensual)", retries_click, backoff_base_ms)
    page.wait_for_timeout(700)

    # Si hay filtro de fecha diaria, intentarlo
    intentar_setear_fecha(page, fecha_objetivo)
    log(f"Estado previo a Planilla: {describir_estado_mensual(page)}")
    return _abrir_mensual_y_descargar_paginado(
        page,
        out_xls=out_xls,
        out_csv=out_csv,
        retries_click=retries_click,
        backoff_base_ms=backoff_base_ms,
        diag_dir=diag_dir,
        session=session,
        hostname=hostname,
        pdf_dir=pdf_dir,
        descargar_pdfs=descargar_pdfs,
    )


def run(playwright: Playwright,
        rut: str,
        clave: str,
        out_path: Path,
        headless: bool,
        fecha: Optional[str],
        dv: Optional[str],
        reintentos_error_sii: int,
        reintentos_click: int,
        backoff_base_ms: int,
        timezone: str = "America/Santiago",
        # --- NUEVO ---
        hostname: Optional[str] = None,
        pdf_dir: Optional[Path] = None,
        descargar_pdfs: Optional[bool] = None,
    ) -> None:

    out_xls = out_path.with_suffix(".xls") if out_path.suffix.lower() != ".xls" else out_path
    out_csv = out_path.with_suffix(".csv")
    diag_dir = out_xls.parent / "_diag"
    if descargar_pdfs is None:
        descargar_pdfs = bool(hostname or pdf_dir)

    log("Parámetros:")
    log(f"  RUT/DV   : {rut} / {dv or '(sin dv)'}")
    log(f"  OUT XLS  : {out_xls}")
    log(f"  OUT CSV  : {out_csv}")
    log(f"  Headless : {headless}")
    log(f"  Fecha    : {fecha or '(no especificada)'}")
    log(f"  Reintentos SII: {reintentos_error_sii}")
    log(
        f"[RUT-CONTROL] proceso={'boletas_pdf' if descargar_pdfs else 'boletas_csv'} "
        f"rut_documentos={rut}-{dv or ''} rut_login={rut}-{dv or ''}"
    )

    browser = playwright.firefox.launch(
        headless=headless,
        slow_mo=150 if not headless else 0
    )
    context = browser.new_context(
        accept_downloads=True,
        locale="es-CL",
        timezone_id=timezone,
        ignore_https_errors=True,
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"),
        viewport={"width": 1366, "height": 860},
    )
    page = context.new_page()
    page.set_default_timeout(45000)
    http_session = requests.Session()

    try:
        navegar_a_consultar_boletas(page, reintentos_click, backoff_base_ms)

        # Intentar login UNA SOLA VEZ (sin reintentos)
        hacer_login(page, rut, dv, clave, reintentos_click, backoff_base_ms)
        estado = esperar_post_login(page, timeout_ms=24000)
        log(f"Estado post-login: {estado}")

        if estado != "ok":
            raise RuntimeError(
                f"No se llegó a la vista de boletas (estado: {estado})."
            )

        resultado = abrir_mensual_y_descargar(
            page,
            out_xls,
            out_csv,
            reintentos_click,
            backoff_base_ms,
            fecha_objetivo=fecha,
            diag_dir=diag_dir,
            session=http_session,
            hostname=hostname,
            pdf_dir=pdf_dir,
            descargar_pdfs=descargar_pdfs,
        )
        log(
            f"Descarga consolidada OK: paginas={resultado.paginas_procesadas}, "
            f"modo={resultado.modo_conversion}"
        )
        if descargar_pdfs:
            log(f"PDFs enviados: {resultado.pdfs_descargados}")

    finally:
        try:
            http_session.close()
        except Exception:
            pass
        try:
            close_sii_session(context, preferred_page=page, log=log)
        except Exception as exc:
            log(f"[SII-SESSION] Fallo cierre de sesion: {exc}")
        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass

    log("OK")


def parse_args():
    parser = argparse.ArgumentParser(description="Exportador SII Boletas Recibidas → XLS/CSV (Planilla)")
    parser.add_argument("--rut", required=True, help="RUT sin puntos (con o sin dígito verificador al final si login 1-caja)")
    parser.add_argument("--dv", help="Dígito verificador (si el login tiene 2 cajas). Si no se entrega y el RUT trae DV al final, se deduce.")
    parser.add_argument("--clave", required=True, help="Clave para el login")
    parser.add_argument("--fecha", help='Fecha objetivo. Acepta "YYYY-MM-DD" o "DD/MM/YYYY". Si no se entrega, baja la planilla mensual.')
    parser.add_argument("--out", required=True, help="Ruta base de salida (si termina en .xls se usa tal cual; también se genera .csv)")
    parser.add_argument("--headless", action="store_true", help="Ejecuta en modo headless")
    parser.add_argument("--reintentos-sii", type=int, default=REINTENTOS_ERROR_SII_DEF, help="Reintentos cuando aparece el error del SII")
    parser.add_argument("--reintentos-click", type=int, default=REINTENTOS_CLICK_DEF, help="Reintentos en clicks críticos")
    parser.add_argument("--backoff-base-ms", type=int, default=BACKOFF_BASE_MS_DEF, help="Base del backoff exponencial en ms")
    parser.add_argument("--timezone", default="America/Santiago", help="Zona horaria del contexto Playwright")
    parser.add_argument("--hostname", help="Nombre del cliente para carpeta de PDFs")
    parser.add_argument("--pdf-dir", help="Carpeta destino para PDFs de boletas")
    parser.add_argument("--descargar-pdfs", action="store_true", help="Forzar descarga de PDFs")

    return parser.parse_args()


if __name__ == "__main__":
    try:
        args = parse_args()
        out_path = Path(args.out)
        with sync_playwright() as p:
            run(
                playwright=p,
                rut=args.rut,
                clave=args.clave,
                out_path=out_path,
                headless=args.headless,
                fecha=args.fecha,
                dv=args.dv,
                reintentos_error_sii=args.reintentos_sii,
                reintentos_click=args.reintentos_click,
                backoff_base_ms=args.backoff_base_ms,
                timezone=args.timezone,
                hostname=getattr(args, "hostname", None),
                pdf_dir=Path(getattr(args, "pdf_dir", "")) if getattr(args, "pdf_dir", None) else None,
                descargar_pdfs=(True if args.descargar_pdfs else None),
            )
    except Exception as e:
        print(f"[BOLETAS][FATAL] {e}", file=sys.stderr)
        sys.exit(1)
