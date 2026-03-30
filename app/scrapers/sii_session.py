"""
Utilidades para cerrar sesion del SII.
Copiado del original exportadorsii/sii_session.py sin modificaciones funcionales.
"""

import re
from typing import Callable, Optional
from urllib.parse import urljoin

LOGOUT_TEXT_RE = re.compile(r"cerrar\s+sesi[oó]n", re.I)
LOGOUT_HREF_RE = re.compile(r"logout|logoff|cerrar|salir|termin", re.I)


def _emit(log: Optional[Callable[[str], None]], message: str) -> None:
    if log is not None:
        try:
            log(message)
        except Exception:
            pass


def _iter_pages(context, preferred_page=None):
    seen: set[int] = set()
    if preferred_page is not None:
        try:
            seen.add(id(preferred_page))
            yield preferred_page
        except Exception:
            pass

    try:
        pages = list(context.pages)
    except Exception:
        pages = []

    for page in reversed(pages):
        pid = id(page)
        if pid in seen:
            continue
        seen.add(pid)
        yield page


def _click_logout_control(page, log) -> bool:
    locator_specs = [
        ("role-link", page.get_by_role("link", name=LOGOUT_TEXT_RE)),
        ("role-button", page.get_by_role("button", name=LOGOUT_TEXT_RE)),
        (
            "css",
            page.locator(
                "a:has-text('Cerrar Sesion'), a:has-text('Cerrar Sesión'), "
                "button:has-text('Cerrar Sesion'), button:has-text('Cerrar Sesión'), "
                "input[value*='Cerrar Sesion' i], input[value*='Cerrar Sesión' i]"
            ),
        ),
    ]

    for label, locator in locator_specs:
        try:
            if locator.count() == 0:
                continue
            candidate = locator.first
            if not candidate.is_visible():
                continue
            _emit(log, f"[SII-SESSION] Cerrando sesion con {label}")
            try:
                candidate.click(timeout=5_000)
            except Exception:
                handle = candidate.element_handle()
                if handle is None:
                    raise
                page.evaluate("(el) => el.click()", handle)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=5_000)
            except Exception:
                pass
            page.wait_for_timeout(1_200)
            return True
        except Exception as exc:
            _emit(log, f"[SII-SESSION] Fallo logout {label}: {exc}")

    return False


def _navigate_logout_href(page, log) -> bool:
    try:
        href = page.evaluate(
            """() => {
                const a = Array.from(document.querySelectorAll('a[href]'));
                const found = a.find(el => {
                    const t = (el.innerText || '').trim();
                    const h = el.getAttribute('href') || '';
                    return /cerrar\\s+sesi[oó]n/i.test(t) || /logout|logoff|cerrar|salir|termin/i.test(h);
                });
                return found ? (found.getAttribute('href') || '') : '';
            }"""
        ) or ""
    except Exception:
        return False

    if not href:
        return False

    target = urljoin(page.url or "", href)
    _emit(log, f"[SII-SESSION] Navegando a logout: {target}")
    try:
        page.goto(target, wait_until="domcontentloaded", timeout=10_000)
    except Exception:
        return False

    try:
        page.wait_for_load_state("networkidle", timeout=5_000)
    except Exception:
        pass
    page.wait_for_timeout(1_200)
    return True


def close_sii_session(context, preferred_page=None, log=None) -> bool:
    closed = False
    for page in _iter_pages(context, preferred_page):
        try:
            if page.is_closed():
                continue
        except Exception:
            continue

        if _click_logout_control(page, log):
            closed = True
            break
        if _navigate_logout_href(page, log):
            closed = True
            break

    try:
        context.clear_cookies()
    except Exception:
        pass

    return closed
