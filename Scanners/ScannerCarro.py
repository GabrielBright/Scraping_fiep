import pandas as pd
import asyncio
import sys
import logging
from playwright.async_api import async_playwright

# Configura encoding e logging
sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def coletar_marcas():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        logging.info("Acessando o site da FIPE...")
        await page.goto('https://veiculos.fipe.org.br/', timeout=120000)

        # Seleciona Motos
        await page.wait_for_selector('li:has-text("Carros e utilitários pequenos")', timeout=30000)
        await page.click('li:has-text("Carros e utilitários pequenos")')

        # Abre dropdown de marcas
        logging.info("Abrindo dropdown de marcas...")
        await page.focus('div.chosen-container#selectMarcacarro_chosen > a')
        await page.click('div.chosen-container#selectMarcacarro_chosen > a')
        await asyncio.sleep(2)
        await page.wait_for_selector('div.chosen-container#selectMarcacarro_chosen ul.chosen-results > li', state='attached', timeout=20000)

        # Captura as marcas
        marcas_elements = await page.query_selector_all('div.chosen-container#selectMarcacarro_chosen ul.chosen-results > li')
        marcas_lista = [await m.text_content() for m in marcas_elements]
        marcas_lista = [m.strip() for m in marcas_lista if m.strip()]

        logging.info(f"Total de marcas encontradas: {len(marcas_lista)}")

        # Salva no Excel
        df = pd.DataFrame(marcas_lista, columns=["Marca"])
        df.to_excel("marcas_Carros.xlsx", index=False)
        logging.info("Arquivo marcas_Carros.xlsx salvo com sucesso!")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(coletar_marcas())
