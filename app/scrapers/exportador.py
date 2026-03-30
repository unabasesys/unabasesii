# exportador.py
import argparse
from datetime import datetime
from pathlib import Path

from playwright.sync_api import Playwright, sync_playwright

from app.scrapers.sii_session import close_sii_session

DOWNLOAD_TIMEOUT_MS = 90_000
DOWNLOAD_RETRIES = 2
DEFAULT_TIMEZONE = "America/Santiago"
UI_SETTLE_MS = 250


def guardar_diagnostico(page, ruta_destino: Path, nombre: str) -> None:
    diag_dir = ruta_destino.parent / "_diag"
    diag_dir.mkdir(parents=True, exist_ok=True)

    try:
        page.screenshot(path=str(diag_dir / f"{nombre}.png"))
    except Exception:
        pass

    try:
        (diag_dir / f"{nombre}.html").write_text(page.content(), encoding="utf-8")
    except Exception:
        pass


def esperar_descarga_csv(page, ruta_destino: Path):
    boton_descarga = page.locator("button:has-text('Descargar Detalles')").first
    boton_descarga.wait_for(state="visible", timeout=45_000)

    for intento in range(1, DOWNLOAD_RETRIES + 1):
        print(f"Esperando descarga del CSV... intento {intento}/{DOWNLOAD_RETRIES}")
        try:
            with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as download_info:
                try:
                    boton_descarga.click(timeout=10_000)
                except Exception:
                    handle = boton_descarga.element_handle()
                    if handle is None:
                        raise
                    page.evaluate("(el) => el.click()", handle)
            return download_info.value
        except Exception as exc:
            print(f"Intento {intento} de descarga CSV fallido: {exc}")
            guardar_diagnostico(page, ruta_destino, f"compras_csv_timeout_intento_{intento}")
            if intento >= DOWNLOAD_RETRIES:
                raise
            page.wait_for_timeout(2_500 * intento)
            boton_descarga.wait_for(state="visible", timeout=45_000)


def run(playwright: Playwright, rut_usuario: str, clave_usuario: str, fecha: str, ruta_csv: str, headless: bool):
    ruta_destino = Path(ruta_csv).expanduser().resolve()

    def parse_mes(fecha_str: str) -> str:
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                f = datetime.strptime(fecha_str.strip(), fmt)
                return f.strftime("%m")
            except ValueError:
                pass
        return datetime.now().strftime("%m")

    mes_str = parse_mes(fecha)

    print("[PY] RUT:", rut_usuario)
    print("[PY] Fecha:", fecha, "-> mes", mes_str)
    print("[PY] CSV destino:", ruta_destino)
    print("[PY] Headless:", headless)
    print(f"[PY][RUT-CONTROL] proceso=compras_csv rut_documentos={rut_usuario} rut_login={rut_usuario}")

    browser = playwright.firefox.launch(headless=headless, slow_mo=200 if not headless else 0)
    context = browser.new_context(
        accept_downloads=True,
        locale="es-CL",
        timezone_id=DEFAULT_TIMEZONE,
        ignore_https_errors=True,
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1366, "height": 860},
    )
    page = context.new_page()
    page.set_default_timeout(45_000)

    try:
        print("Navegando al sitio del SII...")
        page.goto("https://www.sii.cl/servicios_online/1039-.html")

        print("Accediendo al Registro de Compras y Ventas...")
        page.get_by_role("paragraph").filter(has_text="Registro de Compras y Ventas").get_by_role("link").click()
        page.get_by_role("link", name="Ingresar al Registro de").click()

        print("Autenticando usuario...")
        try:
            page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass
        page.wait_for_timeout(UI_SETTLE_MS)

        page.get_by_role("textbox", name="Ej:").fill(rut_usuario)
        page.locator("#clave").fill(clave_usuario)
        page.get_by_role("button", name="Ingresar", exact=True).click()

        print("Esperando respuesta del login...")
        try:
            page.wait_for_load_state("domcontentloaded", timeout=4_000)
        except Exception:
            pass
        page.wait_for_timeout(UI_SETTLE_MS)

        try:
            error_text = page.locator("text=/error|incorrecto|inválido/i").first
            if error_text.is_visible(timeout=2_000):
                error_msg = error_text.text_content()
                print(f"Error de login detectado: {error_msg}")
                raise Exception(f"Error de autenticación: {error_msg}")
        except Exception:
            pass

        print("Esperando carga del formulario después del login...")
        try:
            page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            pass
        page.wait_for_timeout(500)

        print("Buscando selector #periodoMes...")
        try:
            page.wait_for_selector("#periodoMes", timeout=60_000)
            print("Selector #periodoMes encontrado!")
        except Exception as exc:
            print(f"Error: No se encontró el selector #periodoMes: {exc}")
            print("Haciendo captura de pantalla para diagnóstico...")
            try:
                diag_dir = ruta_destino.parent / "_diag"
                diag_dir.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=str(diag_dir / "compras_error_login.png"))
                print(f"Captura guardada en: {diag_dir / 'compras_error_login.png'}")
            except Exception:
                page.screenshot(path="compras_error_login.png")
            raise

        print(f"Seleccionando mes: {mes_str}")
        try:
            page.locator("#periodoMes").select_option(mes_str)
        except Exception:
            page.locator("#periodoMes").select_option(str(int(mes_str)))

        page.get_by_role("button", name="Consultar").click()
        print("Consultando datos del mes...")

        page.wait_for_selector("button:has-text('Descargar Detalles')")

        download = esperar_descarga_csv(page, ruta_destino)

        ruta_destino.parent.mkdir(parents=True, exist_ok=True)
        try:
            ruta_destino.unlink(missing_ok=True)
        except Exception:
            pass
        download.save_as(ruta_destino)

        print("Archivo descargado como:", download.suggested_filename)
        print("Archivo guardado en:", ruta_destino)
        print("Proceso completado correctamente.")
    finally:
        try:
            close_sii_session(context, preferred_page=page, log=print)
        except Exception as exc:
            print(f"[SII-SESSION] Fallo cierre de sesion: {exc}")
        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Exportador SII RCV compras (parametrizado)")
    ap.add_argument("--rut", required=True, help="RUT tal como lo escribes en el formulario (ej: 11111111K o 11111111-1)")
    ap.add_argument("--clave", required=True, help="Clave SII")
    ap.add_argument("--fecha", required=True, help="Fecha dentro del mes (ej. 2025-10-16)")
    ap.add_argument("--csv", required=True, help="Ruta destino del CSV")
    ap.add_argument("--headless", action="store_true", help="Ejecutar navegador en modo headless")
    args = ap.parse_args()

    with sync_playwright() as playwright:
        run(
            playwright=playwright,
            rut_usuario=str(args.rut),
            clave_usuario=args.clave,
            fecha=args.fecha,
            ruta_csv=args.csv,
            headless=args.headless,
        )
