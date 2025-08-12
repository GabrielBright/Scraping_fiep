import asyncio
import json
import logging
import random
import sys
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

async def pausa_curta():
    await asyncio.sleep(random.uniform(0.8, 1.6))

async def pausa_media():
    await asyncio.sleep(random.uniform(2.0, 3.5))

async def pausa_lenta(tag=""):
    t = random.uniform(8, 12)
    logging.info(f"Pausa anti-bloqueio {tag}: {t:.1f}s")
    await asyncio.sleep(t)

async def abrir_dropdown_e_esperar(page, container_id: str):
    # abre o chosen e espera os <li> aparecerem
    await page.click(f"div.chosen-container#{container_id} a.chosen-single", force=True)
    await page.wait_for_selector(
        f"div.chosen-container#{container_id} .chosen-drop ul.chosen-results > li",
        state="visible",
        timeout=10000
    )

async def selecionar_marca_por_nome(page, nome_marca: str, tentativas=3) -> bool:
    for tentativa in range(1, tentativas + 1):
        try:
            await abrir_dropdown_e_esperar(page, "selectMarcamoto_chosen")
            # Input do chosen (campo de busca)
            marca_input = "#selectMarcamoto_chosen .chosen-search input"
            await page.fill(marca_input, nome_marca)
            await pausa_curta()

            itens = await page.query_selector_all("#selectMarcamoto_chosen ul.chosen-results li")
            clicou = False
            for li in itens:
                txt = (await li.inner_text()).strip()
                if txt.lower() == nome_marca.strip().lower():
                    await li.click()
                    clicou = True
                    break

            if not clicou:
                raise RuntimeError("Item exato da marca não apareceu.")

            sel = (await page.locator("#selectMarcamoto_chosen span").inner_text()).strip()
            if nome_marca.lower() in sel.lower():
                return True
            else:
                raise RuntimeError(f"Validação falhou: '{sel}'")

        except Exception as e:
            logging.warning(f"[Marca '{nome_marca}'] Tentativa {tentativa}/{tentativas} falhou: {e}")
            # Fecha e limpa estado, tenta de novo
            await page.keyboard.press("Escape")
            await pausa_media()

    return False

async def coletar_modelos_da_marca(page) -> list[str]:
    await abrir_dropdown_e_esperar(page, "selectAnoModelomoto_chosen")
    lis = await page.query_selector_all("#selectAnoModelomoto_chosen ul.chosen-results > li")
    modelos = []
    for li in lis:
        txt = (await li.inner_text()).strip()
        if txt and "Selecione" not in txt and txt != "-":
            modelos.append(txt)
    await page.keyboard.press("Escape")
    return modelos

async def scan_modelos_por_marca(caminho_excel_marcas="marcas.xlsx", saida_json="catalogo_modelos_motos.json", saida_excel="catalogo_modelos_motos.xlsx"):
    df_marcas = pd.read_excel(caminho_excel_marcas)
    marcas = [str(x).strip() for x in df_marcas["Marca"].tolist() if str(x).strip()]
    logging.info(f"Total de marcas no Excel: {len(marcas)}")

    catalogo = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        logging.info("Acessando o site da FIPE...")
        await page.goto("https://veiculos.fipe.org.br/", timeout=120000)

        # Seleciona a aba Motos
        await page.wait_for_selector('li:has-text("Motos")', timeout=30000)
        await page.click('li:has-text("Motos")')
        await pausa_media()

        for i, marca in enumerate(marcas, start=1):
            logging.info(f"[{i}/{len(marcas)}] Marca: {marca}")

            try:
                ok = await selecionar_marca_por_nome(page, marca)
                if not ok:
                    logging.error(f"[SKIP] Não foi possível selecionar a marca '{marca}' após retries.")
                    await pausa_lenta("marca_skip")
                    continue

                # pequenos waits para página popular o dropdown de modelos
                await pausa_media()

                modelos = await coletar_modelos_da_marca(page)
                logging.info(f"  -> {len(modelos)} modelos coletados para {marca}")
                catalogo[marca] = modelos

                # "Limpar pesquisa" para resetar estado antes da próxima marca
                try:
                    await page.click('#buttonLimparPesquisarmoto a.text', timeout=5000)
                    await pausa_media()
                except PlaywrightTimeoutError:
                    # se não aparecer, tenta manualmente resetar abrindo/fechando
                    await page.keyboard.press("Escape")
                    await pausa_curta()

                # Pausa longa entre marcas pra não ser bloqueado
                await pausa_lenta("entre_marcas")

            except Exception as e:
                logging.error(f"[ERRO] Falha ao coletar modelos da marca '{marca}': {e}")
                # tenta seguir para próxima marca mesmo assim
                await page.keyboard.press("Escape")
                await pausa_lenta("erro_marca")

        await browser.close()

    # Salva JSON + Excel
    Path(saida_json).write_text(
        json.dumps({"_meta": {"total_marcas": len(catalogo)}, "dados": catalogo}, ensure_ascii=False, indent=2), encoding="utf-8")
    logging.info(f"Catálogo salvo em: {saida_json}")

    # Transforma em tabela para Excel: uma linha por (Marca, Modelo)
    linhas = []
    for marca, modelos in catalogo.items():
        for m in modelos:
            linhas.append({"Marca": marca, "Modelo": m})
    pd.DataFrame(linhas).to_excel(saida_excel, index=False)
    logging.info(f"Catálogo (tabular) salvo em: {saida_excel}")

if __name__ == "__main__":
    asyncio.run(scan_modelos_por_marca())