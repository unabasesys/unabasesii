import argparse
import calendar
import html
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Set
from urllib.parse import urlparse

import requests
from playwright.sync_api import Playwright, TimeoutError as PlaywrightTimeoutError, sync_playwright

from app.core.browser import build_chromium_launch_kwargs
from app.scrapers.sii_session import close_sii_session

logger = logging.getLogger(__name__)

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

PDF_CACHE_DIR = Path("downloads") / "_cache"
PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)

URL_DOCS = "https://www1.sii.cl/cgi-bin/Portal001/mipeLaunchPage.cgi?OPCION=1&TIPO=4"
SERVICIOS_ONLINE_URL = "https://www.sii.cl/servicios_online/1039-.html"
BASE_URL = "https://www1.sii.cl"
SAVE_PDF_URL = "https://frank.unabase.com/node/savePdfDocumento"
DEFAULT_TIMEZONE = "America/Santiago"
PAGE_SETTLE_SLEEP_S = 0.25
LOGIN_SETTLE_SLEEP_S = 0.2
DOCUMENTS_TABLE_TIMEOUT_MS = 20_000
DOCUMENT_TARGET_RE = re.compile(
    r"((?:https?://[^'\"\s<>]+)?(?:/cgi-bin/Portal001/|/)?(?:mipeGesDocRcp|mipeShowPdf)\.cgi\?[^'\"\s<>]+)",
    re.I,
)
def formatear_rut(rut_raw: str) -> str:
    rut = rut_raw.replace(".", "").strip()
    if "-" not in rut and len(rut) > 1:
        rut = rut[:-1] + "-" + rut[-1]
    return rut


def safe_filename(text: str, maxlen: int = 40) -> str:
    text = re.sub(r"[^\w\-]", "_", text.strip())
    return text[:maxlen]


def abs_url(href: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return BASE_URL + href
    return BASE_URL + "/" + href


def _cache_path_for_host(hostname: str) -> Path:
    safe_host = re.sub(r"[^\w\-]", "_", (hostname or "sin_hostname").strip()) or "sin_hostname"
    return PDF_CACHE_DIR / f"descargados_{safe_host}.json"


def load_downloaded_cache(hostname: str) -> Set[str]:
    path = _cache_path_for_host(hostname)
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return set(data) if isinstance(data, list) else set()
    except Exception as exc:
        logger.warning("No se pudo leer cache de descargados %s: %s", path, exc)
        return set()


def save_downloaded_cache(hostname: str, cache: Set[str]) -> None:
    path = _cache_path_for_host(hostname)
    try:
        path.write_text(json.dumps(sorted(cache), ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("No se pudo guardar cache de descargados %s: %s", path, exc)


def resolve_periodo_descarga(
    fecha: str,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
) -> tuple[str, str]:
    if fecha_desde and fecha_hasta:
        return fecha_desde, fecha_hasta

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(fecha.strip(), fmt)
            primer_dia = dt.replace(day=1)
            ultimo_dia = dt.replace(day=calendar.monthrange(dt.year, dt.month)[1])
            return primer_dia.strftime("%Y-%m-%d"), ultimo_dia.strftime("%Y-%m-%d")
        except ValueError:
            continue

    raise ValueError(f"Fecha invalida para descarga de compras: {fecha}")


def dump_page_diagnostics(page, label: str, base_dir: Optional[Path] = None) -> None:
    diag_dir = (base_dir or (DOWNLOAD_DIR / "_diag")).resolve()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = safe_filename(label or "diag", maxlen=60) or "diag"
    html_path = diag_dir / f"{base_name}_{stamp}.html"
    png_path = diag_dir / f"{base_name}_{stamp}.png"

    try:
        diag_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        logger.warning("No se pudo crear directorio de diagnostico %s: %s", diag_dir, exc)
        return

    try:
        html = page.content()
        html_path.write_text(html, encoding="utf-8", errors="ignore")
        logger.info("Diagnostico HTML guardado en %s", html_path)
    except Exception as exc:
        logger.warning("No se pudo guardar HTML de diagnostico %s: %s", html_path, exc)

    try:
        page.screenshot(path=str(png_path), full_page=True)
        logger.info("Diagnostico PNG guardado en %s", png_path)
    except Exception as exc:
        logger.warning("No se pudo guardar captura de diagnostico %s: %s", png_path, exc)


def wait_for_page_ready(page, label: str, timeout: int = 30_000) -> None:
    settle_timeout = min(max(timeout // 6, 1_200), 3_000)

    try:
        page.wait_for_load_state("domcontentloaded", timeout=min(timeout, 15_000))
    except PlaywrightTimeoutError:
        logger.warning("[%s] Timeout esperando domcontentloaded. Estado: %s", label, describe_page(page))

    try:
        page.wait_for_load_state("networkidle", timeout=settle_timeout)
        logger.debug("[%s] Estado de carga: networkidle", label)
        return
    except PlaywrightTimeoutError:
        logger.warning("[%s] Timeout esperando networkidle. Reintentando con load. Estado: %s", label, describe_page(page))

    try:
        page.wait_for_load_state("load", timeout=settle_timeout)
        logger.debug("[%s] Estado de carga: load", label)
    except PlaywrightTimeoutError:
        logger.warning("[%s] Tampoco se alcanzo load. Se continua con el estado actual.", label)


def log_context_pages(context, label: str) -> None:
    try:
        pages = []
        for idx, candidate in enumerate(context.pages):
            try:
                pages.append(f"{idx}:{candidate.url}")
            except Exception:
                pages.append(f"{idx}:<sin-url>")
        logger.info("[%s] Paginas abiertas: %s", label, pages)
    except Exception as exc:
        logger.warning("[%s] No se pudieron listar paginas abiertas: %s", label, exc)


def _first_visible_locator(page, selectors: list[str], label: str, require_editable: bool = False):
    for selector in selectors:
        locator = page.locator(selector)
        try:
            count = locator.count()
        except Exception as exc:
            logger.warning("[%s] No se pudo consultar selector %s: %s", label, selector, exc)
            continue

        logger.debug("[%s] Selector '%s' candidatos=%s", label, selector, count)

        for idx in range(count):
            candidate = locator.nth(idx)
            try:
                visible = candidate.is_visible()
                enabled = candidate.is_enabled()
                editable = candidate.is_editable() if require_editable else None
                candidate_info = {
                    "idx": idx,
                    "visible": visible,
                    "enabled": enabled,
                    "editable": editable,
                    "id": candidate.get_attribute("id") or "",
                    "name": candidate.get_attribute("name") or "",
                    "type": candidate.get_attribute("type") or "",
                    "placeholder": candidate.get_attribute("placeholder") or "",
                }
                if idx < 5:
                    logger.debug("[%s] Candidato selector='%s': %s", label, selector, candidate_info)
                if visible and enabled and (not require_editable or editable):
                    logger.info("[%s] Usando selector '%s' idx=%s", label, selector, idx)
                    return candidate
            except Exception as exc:
                logger.warning("[%s] Error evaluando selector '%s' idx=%s: %s", label, selector, idx, exc)

    return None


def goto_target_page(page, context, url: str, label: str, timeout: int = 30_000):
    logger.info("[%s] Navegando a URL objetivo: %s", label, url)
    target_page = page
    target_page.goto(url, wait_until="domcontentloaded")

    wait_for_page_ready(target_page, label, timeout=timeout)
    time.sleep(PAGE_SETTLE_SLEEP_S)
    logger.info("[%s] Estado despues de navegar: %s", label, describe_page(target_page))
    return target_page


def _is_login_page(page) -> bool:
    """Detecta si la pagina actual es la de login del SII (por el path, no query string)."""
    try:
        parsed = urlparse(page.url or "")
        path = parsed.path.lower()
        result = "ingresorutclave" in path or "inicioautenticacion" in path
        logger.debug("[_is_login_page] url=%s path=%s result=%s", page.url, path, result)
        return result
    except Exception:
        return False


def is_cert_error_page(page) -> bool:
    try:
        title = (page.title() or "").strip().lower()
    except Exception:
        title = ""

    try:
        body = (page.text_content("body") or "").strip().lower()
    except Exception:
        body = ""

    return (
        "potential security issue" in title
        or "did not connect" in title
        or "potential security issue" in body
        or "website requires a secure connection" in body
    )


def extract_login_return_url(page) -> str:
    try:
        parsed = urlparse(page.url or "")
        return_url = (parsed.query or "").strip()
        if return_url.startswith("http://") or return_url.startswith("https://"):
            logger.info("[LOGIN] URL de retorno detectada: %s", return_url)
            return return_url
    except Exception as exc:
        logger.warning("[LOGIN] No se pudo extraer URL de retorno: %s", exc)

    logger.info("[LOGIN] No se detecto URL de retorno util en login")
    return ""


def resume_post_login_target(page, context, return_url: str, label_prefix: str):
    candidate_urls = []
    if return_url:
        candidate_urls.append((f"{label_prefix}-return", return_url))
    candidate_urls.append((f"{label_prefix}-url-docs", URL_DOCS))

    target_page = page
    for label, candidate_url in candidate_urls:
        target_page = goto_target_page(target_page, context, candidate_url, label)
        if not _is_login_page(target_page):
            return target_page
        logger.warning("[%s] El target devolvio nuevamente login", label)

    return target_page


def open_compras_entry(page):
    logger.info("Abriendo entrada de Compras y Ventas desde servicios online: %s", SERVICIOS_ONLINE_URL)
    page.goto(SERVICIOS_ONLINE_URL, wait_until="domcontentloaded")
    wait_for_page_ready(page, "compras-services-online", timeout=30_000)
    time.sleep(PAGE_SETTLE_SLEEP_S)

    try:
        page.get_by_role("paragraph").filter(has_text="Registro de Compras y Ventas").get_by_role("link").click()
    except Exception:
        page.get_by_role("link", name=re.compile(r"Registro de Compras y Ventas", re.I)).first.click()

    wait_for_page_ready(page, "compras-registro-link", timeout=30_000)
    time.sleep(PAGE_SETTLE_SLEEP_S)

    page.get_by_role("link", name="Ingresar al Registro de").click()
    wait_for_page_ready(page, "compras-ingresar-registro", timeout=30_000)
    time.sleep(PAGE_SETTLE_SLEEP_S)
    logger.info("Entrada de Compras y Ventas abierta: %s", describe_page(page))
    return page


def launch_browser_for_compras_pdfs(playwright: Playwright, headless: bool):
    slow_mo = 250 if not headless else 0
    browser_name = "chromium"
    try:
        browser = playwright.chromium.launch(
            **build_chromium_launch_kwargs(
                headless=headless,
                slow_mo=slow_mo,
            )
        )
        logger.info("Usando navegador %s para PDFs de compras", browser_name)
        return browser, browser_name
    except Exception as exc:
        logger.warning("No se pudo iniciar navegador %s para PDFs de compras: %s", browser_name, exc)
        raise RuntimeError(
            f"No se pudo iniciar el navegador {browser_name} para PDFs de compras: {exc}"
        ) from exc


def build_compras_pdf_context(browser):
    context = browser.new_context(
        viewport={"width": 1366, "height": 860},
        locale="es-CL",
        timezone_id=DEFAULT_TIMEZONE,
        accept_downloads=True,
        ignore_https_errors=True,
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
    )
    page = context.new_page()
    page.set_default_timeout(45_000)
    return context, page


def _normalize_rut_for_login(value: str) -> str:
    return re.sub(r"[^0-9A-Z]", "", (value or "").upper())


def _read_locator_value(locator) -> str:
    try:
        return locator.input_value()
    except Exception:
        pass

    try:
        return locator.evaluate("el => el.value || ''")
    except Exception:
        return ""


def _write_locator_value(locator, value: str, label: str, compare_as_rut: bool = False) -> bool:
    expected = _normalize_rut_for_login(value) if compare_as_rut else value
    strategies = (
        (
            "fill",
            lambda: (
                locator.click(),
                locator.fill(""),
                locator.fill(value),
            ),
        ),
        (
            "type",
            lambda: (
                locator.click(),
                locator.press("Control+A"),
                locator.press("Delete"),
                locator.type(value, delay=70),
            ),
        ),
        (
            "js",
            lambda: locator.evaluate(
                """(el, v) => {
                    el.focus();
                    el.value = '';
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.value = v;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    if (el.blur) {
                        el.blur();
                    }
                }""",
                value,
            ),
        ),
    )

    for strategy_name, writer in strategies:
        try:
            locator.wait_for(state="visible", timeout=5_000)
        except Exception:
            pass

        try:
            locator.scroll_into_view_if_needed()
        except Exception:
            pass

        try:
            writer()
        except Exception as exc:
            logger.warning("[%s] Fallo estrategia %s escribiendo valor: %s", label, strategy_name, exc)
            continue

        if compare_as_rut:
            try:
                locator.evaluate("el => el.blur && el.blur()")
            except Exception:
                pass

        time.sleep(0.15)
        actual = _read_locator_value(locator)
        actual_comp = _normalize_rut_for_login(actual) if compare_as_rut else actual

        if actual_comp == expected:
            logger.info("[%s] Valor confirmado con %s: %s", label, strategy_name, actual if not compare_as_rut else actual_comp)
            return True

        logger.warning(
            "[%s] Valor no persistio con %s. esperado=%s actual=%s",
            label,
            strategy_name,
            expected,
            actual_comp,
        )

    return False


def _find_login_rut_fields(scope, label_prefix: str):
    rut_field = _first_visible_locator(
        scope,
        selectors=[
            "input[placeholder*='Ej:']:visible",
            "#rutcntr:visible",
            "input[name='rutcntr']:visible",
            "input[placeholder*='RUT' i]:visible",
        ],
        label=f"{label_prefix}-RUT",
        require_editable=True,
    )
    if rut_field is not None:
        return ("single", rut_field, None)

    rut_input = _first_visible_locator(
        scope,
        selectors=["input[name='rut']:visible", "#rut:visible"],
        label=f"{label_prefix}-RUT-PART",
        require_editable=True,
    )
    dv_input = _first_visible_locator(
        scope,
        selectors=["input[name='dv']:visible", "#dv:visible"],
        label=f"{label_prefix}-DV",
        require_editable=True,
    )
    if rut_input is not None and dv_input is not None:
        return ("split", rut_input, dv_input)

    return (None, None, None)


def _log_login_inputs_once(page) -> None:
    try:
        all_inputs = page.evaluate("""() => {
            const inputs = document.querySelectorAll('input, button, select');
            return Array.from(inputs).slice(0, 30).map(el => ({
                tag: el.tagName,
                type: el.type || '',
                id: el.id || '',
                name: el.name || '',
                placeholder: el.placeholder || '',
                value: el.type === 'password' ? '<hidden>' : (el.value || ''),
                className: el.className || '',
                visible: el.offsetParent !== null,
            }));
        }""")
        logger.debug("[LOGIN] Inputs encontrados en pagina: %s", all_inputs)
    except Exception as exc:
        logger.warning("[LOGIN] No se pudieron listar inputs: %s", exc)

    try:
        frames_info = page.evaluate("""() => {
            const frames = document.querySelectorAll('iframe, frame');
            return Array.from(frames).map(f => ({
                id: f.id || '',
                name: f.name || '',
                src: f.src || '',
            }));
        }""")
        if frames_info:
            logger.debug("[LOGIN] Iframes encontrados: %s", frames_info)
    except Exception:
        pass


def _submit_login_in_scope(scope, rut: str, clave_usuario: str, label_prefix: str) -> bool:
    mode, rut_field, dv_field = _find_login_rut_fields(scope, label_prefix)
    if mode is None or rut_field is None:
        return False

    if mode == "single":
        if not _write_locator_value(rut_field, rut, f"{label_prefix}-RUT-VALUE", compare_as_rut=True):
            return False
    else:
        parts = rut.split("-")
        rut_body = parts[0] if parts else rut
        dv = parts[1] if len(parts) > 1 else ""
        if not _write_locator_value(rut_field, rut_body, f"{label_prefix}-RUT-PART-VALUE"):
            return False
        if dv_field is None or not _write_locator_value(dv_field, dv, f"{label_prefix}-DV-VALUE"):
            return False

    clave_field = _first_visible_locator(
        scope,
        selectors=["#clave:visible", "input[type='password']:visible"],
        label=f"{label_prefix}-CLAVE",
        require_editable=True,
    )
    if clave_field is None:
        logger.warning("[%s] No se encontro campo de clave", label_prefix)
        return False

    if not _write_locator_value(clave_field, clave_usuario, f"{label_prefix}-CLAVE-VALUE"):
        return False

    logger.info(
        "[%s] Valores listos para submit: rut=%s clave_len=%s",
        label_prefix,
        _normalize_rut_for_login(_read_locator_value(rut_field)),
        len(_read_locator_value(clave_field)),
    )

    btn = _first_visible_locator(
        scope,
        selectors=[
            "#bt_ingresar:visible",
            "button:has-text('Ingresar'):visible",
            "input[type='submit'][value*='Ingresar' i]:visible",
            "input[type='button'][value*='Ingresar' i]:visible",
        ],
        label=f"{label_prefix}-BTN",
    )
    if btn is None:
        logger.warning("[%s] No se encontro boton de ingresar", label_prefix)
        return False

    try:
        btn_tag = btn.evaluate(
            "el => el.tagName + '#' + (el.id || '') + '.' + (el.className || '') + ' val=' + (el.value || el.textContent || '')"
        )
        logger.info("[%s] Click en boton: %s", label_prefix, btn_tag)
    except Exception:
        pass

    try:
        btn.click(timeout=10_000)
    except Exception:
        btn.evaluate("el => el.click()")

    return True


def _fill_and_submit_login(page, rut_usuario: str, clave_usuario: str, max_attempts: int = 3) -> None:
    """Rellena y envia el formulario de login del SII usando selectores robustos y reintentos sin recargar."""
    rut = formatear_rut(rut_usuario)
    logger.info("Llenando formulario login SII con RUT: %s", rut)

    for attempt in range(1, max_attempts + 1):
        logger.info("[LOGIN] Intento %s/%s", attempt, max_attempts)
        wait_for_page_ready(page, f"login-form-{attempt}", timeout=15_000)
        time.sleep(LOGIN_SETTLE_SLEEP_S)

        if is_cert_error_page(page):
            dump_page_diagnostics(page, "login_cert_error")
            raise RuntimeError(
                "Chromium/Playwright bloqueo la pagina del SII por un problema HTTPS/certificado"
            )

        if attempt == 1:
            _log_login_inputs_once(page)

        logger.debug("[LOGIN] Estado antes de llenar login: %s", describe_page(page))

        submitted = _submit_login_in_scope(page, rut, clave_usuario, f"LOGIN-{attempt}")

        if not submitted:
            logger.debug("[LOGIN] Buscando formulario de login en iframes...")
            for frame_idx, frame in enumerate(page.frames):
                try:
                    logger.debug("[LOGIN] Revisando frame %s: %s", frame_idx, (frame.url or "")[:120])
                    submitted = _submit_login_in_scope(frame, rut, clave_usuario, f"LOGIN-FRAME-{attempt}-{frame_idx}")
                    if submitted:
                        break
                except Exception as exc:
                    logger.warning("[LOGIN] Error revisando frame %s: %s", frame_idx, exc)

        if not submitted:
            try:
                html_snippet = page.evaluate("() => document.documentElement.outerHTML.substring(0, 2000)")
                logger.error("[LOGIN] NO se pudo preparar el formulario. HTML (primeros 2000 chars): %s", html_snippet)
            except Exception:
                pass
            dump_page_diagnostics(page, "login_form_incompleto")
            raise RuntimeError("No se pudo completar el formulario de login SII")

        logger.info("[LOGIN] Formulario enviado. Esperando navegacion...")
        if _wait_for_login_complete(page, timeout=18_000 + (attempt * 4_000)):
            return

        logger.warning(
            "[LOGIN] El SII sigue mostrando login despues del intento %s. Estado actual: %s",
            attempt,
            describe_page(page),
        )
        if attempt < max_attempts:
            time.sleep(0.4 * attempt)

    dump_page_diagnostics(page, "login_reintentos_agotados")
    raise RuntimeError("No se pudo completar login SII tras varios intentos")


def _try_click_continuar(page) -> bool:
    """Intenta detectar y clickear un boton 'Continuar' en pantallas intermedias post-login."""
    try:
        continuar = page.locator(
            "a[href*='siihome.cgi']:has-text('Continuar'), "
            "a.btn-primary:has-text('Continuar'), "
            "button:has-text('Continuar'), "
            "input[value*='Continuar' i]"
        ).first
        if continuar.is_visible():
            logger.info("[LOGIN] Pantalla intermedia detectada. Clickeando 'Continuar'...")
            continuar.click()
            time.sleep(2)
            return True
    except Exception:
        pass
    return False


def _wait_for_login_complete(page, timeout: int = 30_000) -> bool:
    """Espera a que la pagina salga del login y permanezca fuera de esa vista."""
    deadline = time.time() + (timeout / 1000)

    while time.time() < deadline:
        if is_cert_error_page(page):
            dump_page_diagnostics(page, "login_cert_error")
            raise RuntimeError(
                "Chromium/Playwright bloqueo la pagina del SII por un problema HTTPS/certificado"
            )

        # Detectar y saltar pantallas intermedias con boton "Continuar"
        _try_click_continuar(page)

        if not _is_login_page(page):
            try:
                page.wait_for_load_state("networkidle", timeout=1_200)
            except PlaywrightTimeoutError:
                pass
            time.sleep(LOGIN_SETTLE_SLEEP_S)
            if not _is_login_page(page):
                logger.info("[LOGIN] Estado post login: %s", describe_page(page))
                return True

        time.sleep(0.15)

    logger.warning("Timeout esperando salir del login. URL: %s | estado=%s", page.url, describe_page(page))
    return False


def describe_page(page) -> str:
    try:
        url = page.url
    except Exception:
        url = ""

    try:
        title = page.title()
    except Exception:
        title = ""

    try:
        body = (page.text_content("body") or "").strip()
        body = re.sub(r"\s+", " ", body)[:300]
    except Exception:
        body = ""

    return f"url={url} | title={title} | body={body}"


def is_documentos_page(candidate_page) -> bool:
    try:
        url = (candidate_page.url or "").lower()
    except Exception:
        url = ""

    if "mipeadmindocsrcp.cgi" in url or "mipegesdocrcp.cgi" in url:
        return True

    checks = [
        "form[name='FormNameAdmEmi']",
        "#tablaDatos tbody tr",
        "input[name='BTN_SUBMIT']",
        "input[name='FEC_DESDE']",
    ]
    for selector in checks:
        try:
            if candidate_page.query_selector(selector):
                return True
        except Exception:
            pass

    try:
        title_text = candidate_page.text_content("h5.title") or ""
        if "DOCUMENTOS RECIBIDOS" in title_text.upper():
            return True
    except Exception:
        pass

    return False


def find_documentos_page(context, preferred_page):
    candidates = []
    if preferred_page is not None:
        candidates.append(preferred_page)

    for candidate in reversed(list(context.pages)):
        if candidate not in candidates:
            candidates.append(candidate)

    for candidate in candidates:
        try:
            candidate.wait_for_load_state("domcontentloaded", timeout=5_000)
        except Exception:
            pass

        if is_documentos_page(candidate):
            return candidate

    return None


def set_documentos_filters(page, fecha_desde: str, fecha_hasta: str) -> bool:
    return bool(
        page.evaluate(
            """(args) => {
                const form = document.querySelector("form[name='FormNameAdmEmi']");
                const desde = (form && form.querySelector("input[name='FEC_DESDE']")) || document.querySelector("input[name='FEC_DESDE']");
                const hasta = (form && form.querySelector("input[name='FEC_HASTA']")) || document.querySelector("input[name='FEC_HASTA']");
                if (!desde || !hasta) return false;

                desde.value = args.desde;
                hasta.value = args.hasta;
                desde.dispatchEvent(new Event("input", { bubbles: true }));
                desde.dispatchEvent(new Event("change", { bubbles: true }));
                hasta.dispatchEvent(new Event("input", { bubbles: true }));
                hasta.dispatchEvent(new Event("change", { bubbles: true }));
                return true;
            }""",
            {"desde": fecha_desde, "hasta": fecha_hasta},
        )
    )


def _handle_empresa_selection(page, context, rut_usuario: str, rut_empresa: str = ""):
    """Si aterrizamos en mipeSelEmpresa o en un selector de empresas, seleccionar la empresa correcta.

    Args:
        page: pagina actual de Playwright.
        context: contexto del navegador.
        rut_usuario: RUT usado para login (apoderado o contribuyente).
        rut_empresa: RUT de la empresa cuyos documentos se quieren descargar.
                     Si el apoderado tiene varias empresas, este RUT se usa
                     para elegir la correcta en el selector <select>.
    """
    # --- Paso 1: intentar selector <select> de empresas (pantalla con "SELECCI") ---
    rut_buscar = rut_empresa or rut_usuario
    if rut_buscar:
        try:
            has_selector_text = page.evaluate(
                "() => document.body.innerText.includes('SELECCI')"
            )
        except Exception:
            has_selector_text = False

        if has_selector_text:
            rut_buscar_clean = rut_buscar.replace(".", "").strip()
            # Normalizar: asegurar formato con guion para buscar en opciones
            rut_buscar_formatted = formatear_rut(rut_buscar_clean)
            logger.info(
                "Pantalla con selector de empresa detectada. Buscando RUT empresa: %s",
                rut_buscar_formatted,
            )

            seleccionado = page.evaluate(
                """(rut) => {
                    const selects = document.querySelectorAll('select');
                    for (const select of selects) {
                        for (let i = 0; i < select.options.length; i++) {
                            const optText = select.options[i].text || '';
                            const optValue = select.options[i].value || '';
                            // Buscar RUT con o sin puntos/guion
                            const rutClean = rut.replace(/\\./g, '').replace('-', '');
                            if (optText.includes(rut) || optValue.includes(rut)
                                || optText.replace(/\\./g, '').replace('-', '').includes(rutClean)
                                || optValue.replace(/\\./g, '').replace('-', '').includes(rutClean)) {
                                select.selectedIndex = i;
                                select.dispatchEvent(new Event('change', { bubbles: true }));
                                return { found: true, option: optText.substring(0, 80) };
                            }
                        }
                    }
                    return { found: false };
                }""",
                rut_buscar_formatted,
            )

            if seleccionado and seleccionado.get("found"):
                logger.info(
                    "Empresa seleccionada en dropdown: %s", seleccionado.get("option", "")
                )
                # Buscar y clickear boton Enviar/Aceptar
                btn_enviar = _first_visible_locator(
                    page,
                    selectors=[
                        "button:has-text('Enviar'):visible",
                        "input[value='Enviar']:visible",
                        "a:has-text('Enviar'):visible",
                        "button:has-text('Aceptar'):visible",
                        "input[value='Aceptar']:visible",
                        "input[type='submit']:visible",
                    ],
                    label="empresa-select-submit",
                )
                if btn_enviar:
                    try:
                        btn_enviar.click()
                        wait_for_page_ready(page, "empresa-select-post-submit", timeout=30_000)
                        time.sleep(PAGE_SETTLE_SLEEP_S)
                        logger.info("Post seleccion empresa (select). URL: %s", page.url)
                    except Exception as e:
                        logger.warning("No se pudo hacer clic en boton Enviar del selector: %s", e)
                else:
                    logger.warning("Empresa seleccionada pero no se encontro boton Enviar/Aceptar")
            else:
                logger.warning(
                    "No se encontro la empresa %s en el selector <select>. "
                    "Se continuara con la seleccion por defecto.",
                    rut_buscar_formatted,
                )

    # --- Paso 2: manejar pagina mipeSelEmpresa (seleccion por links) ---
    try:
        parsed = urlparse(page.url or "")
        path = parsed.path.lower()
    except Exception:
        return page

    if "mipeselempresa" not in path:
        return page

    logger.info("Pagina de seleccion de empresa detectada. URL: %s", page.url)
    page.wait_for_load_state("domcontentloaded", timeout=15_000)
    time.sleep(PAGE_SETTLE_SLEEP_S)

    logger.info("Contenido pagina empresa: %s", describe_page(page))

    # Usar rut_empresa para buscar la empresa correcta; fallback a rut_usuario
    rut_for_link = rut_empresa or rut_usuario
    rut_clean = rut_for_link.replace(".", "").replace("-", "").strip()
    try:
        empresa_link = None

        # 1. Buscar por RUT en href
        empresa_link = page.query_selector(f"a[href*='{rut_clean}']")

        # 2. Buscar por RUT formateado en texto
        if not empresa_link:
            rut_formatted = formatear_rut(rut_for_link)
            empresa_link = page.query_selector(f"a:has-text('{rut_formatted}')")

        # 3. Buscar links al portal
        if not empresa_link:
            empresa_link = page.query_selector(
                "a[href*='mipeLaunchPage'], a[href*='mipeAdminDocsRcp'], "
                "a[href*='mipeGesDocRcp'], a[href*='Portal001']"
            )

        # 4. Buscar cualquier link en tabla
        if not empresa_link:
            all_links = page.query_selector_all("table a[href], td a[href], .box a[href], form a[href]")
            for link in all_links:
                href = (link.get_attribute("href") or "").lower()
                text = (link.inner_text() or "").strip()
                if href and "sii.cl" in href and text:
                    empresa_link = link
                    break

        # 5. Buscar botones
        if not empresa_link:
            empresa_link = page.query_selector(
                "input[type='submit'], input[type='button'][onclick], "
                "button[onclick], input[value*='Seleccionar'], input[value*='Ingresar']"
            )

        if empresa_link:
            tag = empresa_link.evaluate("el => el.tagName").lower()
            href_or_text = empresa_link.get_attribute("href") or empresa_link.inner_text() or ""
            logger.info("Seleccionando empresa. tag=%s, ref=%s", tag, href_or_text[:80])
            try:
                with context.expect_page(timeout=8_000) as popup_info:
                    empresa_link.click()
                result_page = popup_info.value
            except PlaywrightTimeoutError:
                result_page = page
            wait_for_page_ready(result_page, "empresa-selection", timeout=30_000)
            time.sleep(PAGE_SETTLE_SLEEP_S)
            logger.info("Post empresa selection. URL: %s", result_page.url)
            return result_page
        else:
            all_a = page.query_selector_all("a[href]")
            hrefs = [a.get_attribute("href") or "" for a in all_a[:20]]
            logger.warning("No se encontro link de empresa en mipeSelEmpresa. Links: %s", hrefs[:10])
    except Exception as exc:
        logger.warning("Error seleccionando empresa: %s", exc)

    return page


def buscar_documentos(
    page,
    context,
    fecha_desde: str,
    fecha_hasta: str,
    rut_usuario: str = "",
    clave_usuario: str = "",
    rut_empresa: str = "",
):
    logger.info(
        "[BUSCAR-DOCS] Buscando documentos recibidos SII entre %s y %s | rut_login(apoderado)=%s | rut_empresa=%s",
        fecha_desde,
        fecha_hasta,
        rut_usuario,
        rut_empresa or "(no proporcionado, se usara rut_login)",
    )

    # Manejar seleccion de empresa si el login nos dejo ahi
    page = _handle_empresa_selection(page, context, rut_usuario, rut_empresa=rut_empresa)

    # Navegar a documentos recibidos
    target_page = goto_target_page(page, context, URL_DOCS, "buscar-documentos-open")

    # Si caimos en login, autenticarse ahi (la URL de retorno ya esta correcta)
    if _is_login_page(target_page):
        logger.info("Redireccion a login detectada. Autenticando en esta pagina.")
        return_url = extract_login_return_url(target_page)
        _fill_and_submit_login(target_page, rut_usuario, clave_usuario)
        _wait_for_login_complete(target_page)
        logger.info("Post login en redireccion. URL: %s", target_page.url)

        target_page = resume_post_login_target(
            target_page,
            context,
            return_url=return_url,
            label_prefix="buscar-documentos-resume",
        )

        # Manejar empresa selection post login
        target_page = _handle_empresa_selection(target_page, context, rut_usuario, rut_empresa=rut_empresa)

        # Si aun no estamos en docs, navegar de nuevo
        if not is_documentos_page(target_page):
            logger.info("Navegando nuevamente a URL_DOCS")
            target_page = goto_target_page(target_page, context, URL_DOCS, "buscar-documentos-reopen")

        if _is_login_page(target_page):
            raise RuntimeError(
                "El SII devolvio nuevamente la pagina de login al intentar abrir documentos recibidos. "
                + describe_page(target_page)
            )

    try:
        target_page.wait_for_selector(
            "input[name='FEC_DESDE']",
            timeout=40_000,
            state="attached",
        )
    except PlaywrightTimeoutError:
        alternate_page = find_documentos_page(context, target_page)
        if alternate_page is None:
            raise RuntimeError(
                "No se encontro el formulario de fechas de documentos recibidos. "
                + describe_page(target_page)
            )

        if alternate_page != target_page:
            logger.info(
                "Se detecto pagina alternativa de documentos recibidos: %s",
                alternate_page.url,
            )
            target_page = alternate_page
            wait_for_page_ready(target_page, "buscar-documentos-alternate", timeout=30_000)
            time.sleep(PAGE_SETTLE_SLEEP_S)

        target_page.wait_for_selector(
            "input[name='FEC_DESDE']",
            timeout=20_000,
            state="attached",
        )

    logger.info("Pagina de documentos recibidos OK: %s", describe_page(target_page))

    if not set_documentos_filters(target_page, fecha_desde, fecha_hasta):
        raise RuntimeError(
            "Se detecto la pagina de documentos recibidos, pero no fue posible ubicar FEC_DESDE/FEC_HASTA"
        )

    target_page.evaluate("verificaCampos()")

    try:
        target_page.wait_for_selector("#tablaDatos tbody tr td", timeout=DOCUMENTS_TABLE_TIMEOUT_MS)
    except PlaywrightTimeoutError as exc:
        raise RuntimeError("No se cargaron datos en la tabla de compras PDF.") from exc

    time.sleep(PAGE_SETTLE_SLEEP_S)
    return target_page


def _extract_document_target(raw_value: str) -> str:
    value = html.unescape((raw_value or "").strip())
    if not value:
        return ""

    match = DOCUMENT_TARGET_RE.search(value)
    if match:
        return match.group(1)

    if ("mipeGesDocRcp" in value or "mipeShowPdf" in value) and not value.lower().startswith("javascript:"):
        return value

    return ""


def _collect_document_rows(page) -> list[Dict[str, Any]]:
    return page.evaluate(
        """() => {
            const rows = Array.from(document.querySelectorAll("#tablaDatos tbody tr"));
            return rows.map((row, rowIndex) => ({
                rowIndex,
                cells: Array.from(row.querySelectorAll("td")).map((td) => (td.textContent || "").trim()),
                actions: Array.from(row.querySelectorAll("a, button, input, [onclick]")).slice(0, 20).map((el) => ({
                    href: el.getAttribute("href") || "",
                    onclick: el.getAttribute("onclick") || "",
                    value: el.getAttribute("value") || "",
                    text: (el.textContent || "").trim(),
                })),
            }));
        }"""
    ) or []


def _resolve_document_href(row_snapshot: Dict[str, Any]) -> str:
    for action in row_snapshot.get("actions", []):
        for raw in (
            action.get("href", ""),
            action.get("onclick", ""),
            action.get("value", ""),
            action.get("text", ""),
        ):
            target = _extract_document_target(raw)
            if target:
                return target
    return ""


def _build_document_filename(cells: list[str], fallback_index: int) -> str:
    try:
        emisor = safe_filename(cells[1], 15) if len(cells) > 1 else "emisor"
        tipo_doc = safe_filename(cells[3], 20) if len(cells) > 3 else "documento"
        folio = safe_filename(cells[4], 10) if len(cells) > 4 else str(fallback_index)
        fecha = safe_filename(cells[5], 10) if len(cells) > 5 else "sin_fecha"
        return f"{fecha}_{emisor}_{tipo_doc}_{folio}.pdf"
    except Exception:
        return f"documento_{fallback_index}.pdf"


def _log_document_snapshot_debug(page_num: int, row_snapshots: list[Dict[str, Any]]) -> None:
    preview = []
    for row_snapshot in row_snapshots[:5]:
        preview.append(
            {
                "rowIndex": row_snapshot.get("rowIndex"),
                "cells": row_snapshot.get("cells", [])[:6],
                "actions": row_snapshot.get("actions", [])[:3],
            }
        )
    logger.warning(
        "No se detectaron links de detalle/PDF en pagina %s. Preview filas=%s",
        page_num,
        preview,
    )


def _get_datatables_page_info(page) -> tuple[Optional[int], Optional[int]]:
    """Extrae pagina actual y total de paginas del componente DataTables."""
    try:
        info = page.evaluate("""() => {
            // DataTables muestra info como "Showing 1 to 10 of 25 entries" o similar
            const info = document.querySelector('#tablaDatos_info');
            if (!info) return null;
            const text = info.textContent || '';
            // Intentar extraer total de registros
            const m = text.match(/(\\d+)\\s+de\\s+(\\d+)/i) || text.match(/of\\s+(\\d+)/i);

            // Detectar pagina activa
            const activePage = document.querySelector('#tablaDatos_paginate .paginate_button.current, #tablaDatos_paginate .paginate_active');
            const currentPage = activePage ? parseInt(activePage.textContent) : null;

            // Contar total de paginas por botones numericos
            const pageButtons = document.querySelectorAll('#tablaDatos_paginate .paginate_button:not(.previous):not(.next):not(.first):not(.last):not(#tablaDatos_previous):not(#tablaDatos_next):not(#tablaDatos_first):not(#tablaDatos_last)');
            const totalPages = pageButtons.length || null;

            return { currentPage, totalPages };
        }""")
        if info:
            return info.get("currentPage"), info.get("totalPages")
    except Exception as exc:
        logger.debug("No se pudo obtener info de paginacion DataTables: %s", exc)
    return None, None


def obtener_documentos(page) -> list[Dict[str, str]]:
    documentos: list[Dict[str, str]] = []
    page_num = 1
    visited_pages: set[str] = set()
    max_pages = 100  # limite de seguridad

    while page_num <= max_pages:
        logger.info("Leyendo pagina %s de documentos recibidos", page_num)

        # Obtener info de paginacion para logging
        dt_current, dt_total = _get_datatables_page_info(page)
        if dt_current or dt_total:
            logger.info(
                "DataTables pagina actual=%s total_paginas=%s",
                dt_current or "?",
                dt_total or "?",
            )

        # Capturar contenido de la primera fila antes del cambio de pagina (para verificar cambios)
        first_row_text = ""
        try:
            first_row = page.query_selector("#tablaDatos tbody tr:first-child")
            if first_row:
                first_row_text = (first_row.inner_text() or "").strip()[:200]
        except Exception:
            pass

        row_snapshots = _collect_document_rows(page)
        page_documents = 0

        for row_snapshot in row_snapshots:
            href = _resolve_document_href(row_snapshot)
            if not href:
                continue

            filename = _build_document_filename(
                row_snapshot.get("cells", []),
                fallback_index=len(documentos) + 1,
            )
            documentos.append({"href": href, "filename": filename})
            page_documents += 1

        logger.info(
            "Pagina %s de documentos recibidos: filas=%s documentos_detectados=%s",
            page_num,
            len(row_snapshots),
            page_documents,
        )
        if row_snapshots and page_documents == 0:
            _log_document_snapshot_debug(page_num, row_snapshots)

        # --- Estrategia 1: link #pagina_siguiente (paginacion server-side) ---
        next_link = page.query_selector("#pagina_siguiente[href]")
        if next_link:
            next_href = next_link.get_attribute("href") or ""
            next_url = abs_url(next_href) if next_href else ""
            if not next_url or next_url in visited_pages:
                logger.info("No hay mas paginas (link ya visitado o vacio)")
                break

            visited_pages.add(next_url)
            page.goto(next_url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_selector("#tablaDatos tbody tr", timeout=30_000)
            time.sleep(PAGE_SETTLE_SLEEP_S)
            page_num += 1
            continue

        # --- Estrategia 2: boton DataTables #tablaDatos_next (paginacion client-side) ---
        next_btn = page.query_selector("#tablaDatos_next:not(.disabled)")
        if not next_btn:
            logger.info("No hay mas paginas (boton next deshabilitado o no existe)")
            break

        logger.info("Clickeando boton next de DataTables para ir a pagina %s", page_num + 1)
        next_btn.click()

        # Esperar a que la tabla se actualice verificando que el contenido cambie
        try:
            page.wait_for_selector("#tablaDatos tbody tr td", timeout=30_000)
        except PlaywrightTimeoutError:
            logger.warning("Timeout esperando datos en tabla despues de click next")
            break

        # Esperar a que el contenido de la tabla realmente cambie
        if first_row_text:
            deadline = time.time() + 5
            content_changed = False
            while time.time() < deadline:
                try:
                    new_first_row = page.query_selector("#tablaDatos tbody tr:first-child")
                    if new_first_row:
                        new_text = (new_first_row.inner_text() or "").strip()[:200]
                        if new_text != first_row_text:
                            content_changed = True
                            break
                except Exception:
                    pass
                time.sleep(0.3)

            if not content_changed:
                logger.warning(
                    "El contenido de la tabla no cambio despues de click next. "
                    "Posible ultima pagina alcanzada."
                )
                break

        time.sleep(PAGE_SETTLE_SLEEP_S)
        page_num += 1

    if page_num > max_pages:
        logger.warning("Se alcanzo el limite de seguridad de %s paginas", max_pages)

    logger.info("Total documentos encontrados para compras PDF: %s", len(documentos))
    return documentos


def extraer_pdf_url(detail_page) -> str:
    return detail_page.evaluate(
        r"""
        () => {
            const link = document.querySelector("a[href*='mipeShowPdf']");
            if (link) return link.getAttribute('href');

            const inputs = document.querySelectorAll("input[onclick]");
            for (const el of inputs) {
                const oc = el.getAttribute('onclick') || '';
                const m = oc.match(/['"]([\/\w\-\.\?\=\&]+mipeShowPdf[^'"]*)['"]/);
                if (m) return m[1];
            }

            const all = document.querySelectorAll('[onclick]');
            for (const el of all) {
                const oc = el.getAttribute('onclick') || '';
                const m = oc.match(/['"]([\/\w\-\.\?\=\&]+mipeShowPdf[^'"]*)['"]/);
                if (m) return m[1];
            }

            const match = document.body.innerHTML.match(
                /(\/cgi-bin\/Portal001\/mipeShowPdf\.cgi\?[^"'\s<>]+)/
            );
            if (match) return match[1];

            return null;
        }
        """
    ) or ""


def enviar_pdf_a_api(
    pdf_bytes: bytes,
    nombre_archivo: str,
    hostname: Optional[str],
    session: requests.Session,
) -> bool:
    try:
        files = {
            "file": (nombre_archivo, pdf_bytes, "application/pdf"),
        }
        data = {
            "filename": nombre_archivo,
            "hostname": hostname or "",
        }

        response = session.post(
            SAVE_PDF_URL,
            files=files,
            data=data,
            timeout=(10, 60),
        )

        if 200 <= response.status_code < 300:
            return True

        logger.warning(
            "savePdfDocumento respondio %s para %s: %s",
            response.status_code,
            nombre_archivo,
            response.text[:300],
        )
        return False
    except Exception as exc:
        logger.warning("Error enviando PDF de compras %s: %s", nombre_archivo, exc)
        return False


def descargar_pdf(
    context,
    href: str,
    filename: str,
    hostname: Optional[str] = None,
    session: Optional[requests.Session] = None,
    save_path: Optional[Path] = None,
) -> bool:
    target_url = abs_url(href)
    detail_page = None

    try:
        pdf_url = target_url if "mipeShowPdf" in target_url else ""

        if not pdf_url:
            detail_page = context.new_page()
            detail_page.goto(target_url, wait_until="domcontentloaded", timeout=30_000)
            detail_page.wait_for_selector(
                "[href*='mipeShowPdf'], [onclick*='mipeShowPdf']",
                timeout=10_000,
                state="attached",
            )

            pdf_url = extraer_pdf_url(detail_page)

            if not pdf_url or "mipeShowPdf" not in pdf_url:
                pdf_btn = (
                    detail_page.query_selector("input[value*='VISUALIZACI']")
                    or detail_page.query_selector("a[href*='mipeShowPdf']")
                    or detail_page.query_selector("button:has-text('VISUALIZACI')")
                )
                if pdf_btn:
                    try:
                        with context.expect_page(timeout=6_000) as popup_info:
                            detail_page.evaluate("el => el.click()", pdf_btn)
                        popup = popup_info.value
                        popup.wait_for_load_state("domcontentloaded", timeout=10_000)
                        pdf_url = popup.url
                        popup.close()
                    except PlaywrightTimeoutError:
                        pass

        if not pdf_url or "mipeShowPdf" not in str(pdf_url):
            logger.warning("No se pudo obtener URL PDF para %s", filename)
            return False

        response = context.request.get(abs_url(pdf_url))
        if response.status != 200:
            logger.warning("Respuesta %s descargando PDF %s", response.status, filename)
            return False

        content = response.body()
        if content[:4] != b"%PDF":
            logger.warning("Contenido invalido al descargar PDF %s", filename)
            return False

        uploaded_ok = False
        if session is not None:
            uploaded_ok = enviar_pdf_a_api(content, filename, hostname, session)
            if uploaded_ok:
                logger.info("PDF de compras enviado correctamente: %s", filename)

        saved_ok = False
        if save_path is not None:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_bytes(content)
            saved_ok = True

        return uploaded_ok or saved_ok or (session is None and save_path is None)
    except Exception as exc:
        logger.warning("Error descargando PDF %s: %s", filename, exc)
        return False
    finally:
        try:
            if detail_page is not None:
                detail_page.close()
        except Exception:
            pass


def download_and_upload_compras_pdfs(
    playwright: Playwright,
    rut_usuario: str,
    clave_usuario: str,
    fecha: str,
    hostname: Optional[str] = None,
    rut_apoderado: Optional[str] = None,
    dv_apoderado: Optional[str] = None,
    clave_apoderado: Optional[str] = None,
    headless: bool = True,
    pdf_dir: Optional[Path] = None,
    save_local: bool = False,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
) -> Dict[str, Any]:
    # --- DESACTIVADO TEMPORALMENTE: descarga de PDF de compras ---
    logger.info("[COMPRAS-PDF] Logica de descarga de PDF de compras desactivada temporalmente.")
    return {
        "status": "disabled",
        "total": 0,
        "uploaded": 0,
        "periodo": {"desde": "", "hasta": ""},
        "message": "Descarga de PDF de compras desactivada temporalmente.",
    }
    # --- FIN DESACTIVADO ---

    periodo_desde, periodo_hasta = resolve_periodo_descarga(
        fecha=fecha,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
    )

    missing_apoderado_fields = []
    if not (rut_apoderado or "").strip():
        missing_apoderado_fields.append("rut_apoderado")
    if not (dv_apoderado or "").strip():
        missing_apoderado_fields.append("dv_apoderado")
    if not (clave_apoderado or "").strip():
        missing_apoderado_fields.append("clave_apoderado")

    if missing_apoderado_fields:
        message = (
            "No se ejecutaran los PDFs de compras porque faltan datos de apoderado: "
            + ", ".join(missing_apoderado_fields)
        )
        logger.error(message)
        return {
            "status": "skipped_missing_apoderado_credentials",
            "total": 0,
            "uploaded": 0,
            "periodo": {
                "desde": periodo_desde,
                "hasta": periodo_hasta,
            },
            "message": message,
        }

    browser, browser_name = launch_browser_for_compras_pdfs(
        playwright=playwright,
        headless=headless,
    )
    context, page = build_compras_pdf_context(browser)
    session = requests.Session() if hostname is not None else None

    result: Dict[str, Any] = {
        "status": "pending",
        "total": 0,
        "uploaded": 0,
        "periodo": {
            "desde": periodo_desde,
            "hasta": periodo_hasta,
        },
    }

    login_rut = (rut_apoderado or "").strip()
    if dv_apoderado and "-" not in login_rut:
        login_rut = f"{login_rut}-{dv_apoderado.strip()}"
    login_rut = formatear_rut(login_rut)
    login_clave = clave_apoderado or ""
    using_apoderado = True

    try:
        logger.info(
            "Contexto listo para PDFs de compras con navegador=%s headless=%s",
            browser_name,
            headless,
        )
        logger.info(
            "[RUT-CONTROL][compras_pdf_newlogic] "
            "rut_empresa(documentos)=%s | rut_login(apoderado)=%s | usando_apoderado=%s | headless=%s | hostname=%s",
            rut_usuario,
            login_rut,
            using_apoderado,
            headless,
            hostname or "",
        )
        logger.info(
            "[RUT-DETALLE] El login se hara con el RUT del apoderado: %s. "
            "La seleccion de empresa en el selector usara el RUT: %s",
            login_rut,
            rut_usuario,
        )
        logger.info("Intentando estrategia directa a URL_DOCS para PDFs de compras")
        try:
            docs_page = buscar_documentos(
                page,
                context,
                periodo_desde,
                periodo_hasta,
                rut_usuario=login_rut,
                clave_usuario=login_clave,
                rut_empresa=rut_usuario,
            )
        except Exception as direct_exc:
            logger.warning("Fallo estrategia directa a URL_DOCS: %s", direct_exc)
            dump_page_diagnostics(page, "compras_direct_strategy_error")

            try:
                close_sii_session(
                    context,
                    preferred_page=page,
                    log=lambda msg: logger.info(msg),
                )
            except Exception as close_exc:
                logger.warning("Fallo cierre de sesion previo al fallback de compras PDF: %s", close_exc)

            try:
                context.close()
            except Exception:
                pass

            context, page = build_compras_pdf_context(browser)
            logger.info("Reintentando PDFs de compras via servicios online")

            page = open_compras_entry(page)

            if _is_login_page(page):
                logger.info("Pagina de login detectada en fallback, autenticando...")
                return_url = extract_login_return_url(page)
                _fill_and_submit_login(page, login_rut, login_clave)
                _wait_for_login_complete(page)
                logger.info("Post login fallback. URL: %s", page.url)
                page = resume_post_login_target(
                    page,
                    context,
                    return_url=return_url,
                    label_prefix="compras-fallback-resume",
                )

            page = _handle_empresa_selection(page, context, login_rut, rut_empresa=rut_usuario)
            docs_page = buscar_documentos(
                page,
                context,
                periodo_desde,
                periodo_hasta,
                rut_usuario=login_rut,
                clave_usuario=login_clave,
                rut_empresa=rut_usuario,
            )

        documentos = obtener_documentos(docs_page)
        result["total"] = len(documentos)

        if not documentos:
            dump_page_diagnostics(docs_page, "compras_pdf_no_documents")
            result["status"] = "no_documents"
            return result

        # Cargar cache de PDFs ya descargados para este hostname
        cache_key = hostname or rut_usuario
        downloaded_cache = load_downloaded_cache(cache_key)
        logger.info(
            "[CACHE] Cache cargada para '%s': %s PDFs previamente descargados",
            cache_key,
            len(downloaded_cache),
        )

        # Filtrar documentos ya descargados
        documentos_nuevos = [doc for doc in documentos if doc["filename"] not in downloaded_cache]
        skipped = len(documentos) - len(documentos_nuevos)
        if skipped > 0:
            logger.info(
                "[CACHE] Saltando %s/%s PDFs ya descargados previamente. Quedan %s por descargar.",
                skipped,
                len(documentos),
                len(documentos_nuevos),
            )
        result["skipped_cached"] = skipped

        if not documentos_nuevos:
            logger.info("[CACHE] Todos los PDFs ya fueron descargados previamente. Nada que hacer.")
            result["uploaded"] = 0
            result["status"] = "ok"
            return result

        destino_dir = pdf_dir or DOWNLOAD_DIR
        ok = 0

        for index, doc in enumerate(documentos_nuevos, start=1):
            logger.info(
                "Descargando PDF compra %s/%s: %s",
                index,
                len(documentos_nuevos),
                doc["filename"],
            )

            save_path = None
            if save_local:
                save_path = destino_dir / doc["filename"]

            if descargar_pdf(
                context=context,
                href=doc["href"],
                filename=doc["filename"],
                hostname=hostname,
                session=session,
                save_path=save_path,
            ):
                ok += 1
                downloaded_cache.add(doc["filename"])

            time.sleep(0.1)

        # Guardar cache actualizada
        save_downloaded_cache(cache_key, downloaded_cache)
        logger.info("[CACHE] Cache actualizada para '%s': %s PDFs totales registrados", cache_key, len(downloaded_cache))

        result["uploaded"] = ok
        result["status"] = "ok"
        return result
    except Exception as exc:
        logger.error("Error en descarga de PDFs de compras: %s", exc)
        logger.error("Estado final de pagina principal: %s", describe_page(page))
        log_context_pages(context, "compras-pdf-error")
        dump_page_diagnostics(page, "compras_pdf_error")
        raise
    finally:
        try:
            if session is not None:
                session.close()
        except Exception:
            pass
        try:
            close_sii_session(
                context,
                preferred_page=page,
                log=lambda msg: logger.info(msg),
            )
        except Exception as exc:
            logger.warning("Fallo cierre de sesion SII en compras PDF: %s", exc)
        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass


def parse_args():
    parser = argparse.ArgumentParser(description="Descarga y envia PDFs de compras del SII")
    parser.add_argument("--rut", required=True, help="RUT completo del contribuyente, con o sin guion")
    parser.add_argument("--clave", required=True, help="Clave del SII")
    parser.add_argument("--fecha", required=True, help="Fecha base en formato YYYY-MM-DD")
    parser.add_argument("--hostname", help="Hostname del cliente para enviar PDFs")
    parser.add_argument("--rut-apoderado", dest="rut_apoderado", help="RUT del apoderado")
    parser.add_argument("--dv-apoderado", dest="dv_apoderado", help="DV del apoderado")
    parser.add_argument("--clave-apoderado", dest="clave_apoderado", help="Clave del apoderado")
    parser.add_argument("--fecha-desde", dest="fecha_desde", help="Fecha inicial del rango YYYY-MM-DD")
    parser.add_argument("--fecha-hasta", dest="fecha_hasta", help="Fecha final del rango YYYY-MM-DD")
    parser.add_argument("--pdf-dir", dest="pdf_dir", help="Carpeta local opcional para guardar PDFs")
    parser.add_argument("--save-local", action="store_true", help="Guardar PDFs tambien en disco")
    parser.add_argument("--headed", action="store_true", help="Ejecutar navegador visible")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    with sync_playwright() as playwright:
        result = download_and_upload_compras_pdfs(
            playwright=playwright,
            rut_usuario=args.rut,
            clave_usuario=args.clave,
            fecha=args.fecha,
            hostname=args.hostname,
            rut_apoderado=args.rut_apoderado,
            dv_apoderado=args.dv_apoderado,
            clave_apoderado=args.clave_apoderado,
            headless=not args.headed,
            pdf_dir=Path(args.pdf_dir) if args.pdf_dir else DOWNLOAD_DIR,
            save_local=args.save_local,
            fecha_desde=args.fecha_desde,
            fecha_hasta=args.fecha_hasta,
        )
        print(result)


if __name__ == "__main__":
    main()
