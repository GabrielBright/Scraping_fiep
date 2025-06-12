import pandas as pd
import os
import asyncio
import sys
import logging
from tqdm import tqdm
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# Configura encoding e logging
sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

Fipe = []

async def abrir_dropdown_e_esperar(page, container_id):
    logging.info(f"Abrindo dropdown: {container_id}")
    await page.focus(f'div.chosen-container#{container_id} > a')
    await page.click(f'div.chosen-container#{container_id} > a')
    await asyncio.sleep(2)
    await page.wait_for_selector(f'div.chosen-container#{container_id} ul.chosen-results > li', state='attached', timeout=20000)

async def selecionar_item_por_index(page, container_id, index, use_arrow=False):
    logging.info(f"Selecionando item {index+1} no dropdown {container_id}")
    await abrir_dropdown_e_esperar(page, container_id)
    await page.focus(f'div.chosen-container#{container_id} > a')
    await asyncio.sleep(0.5)

    if use_arrow:
        await page.keyboard.press("Home")
        await asyncio.sleep(0.3)
        for _ in range(index):
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(0.3)
        await page.keyboard.press("Enter")
        await asyncio.sleep(1)
    else:
        items = await page.query_selector_all(f'div.chosen-container#{container_id} ul.chosen-results > li')
        if not items:
            logging.warning(f"Dropdown {container_id} não carregou itens!")
            return
        if index >= len(items):
            logging.warning(f"Index {index} fora do range no dropdown {container_id} (total: {len(items)})")
            return
        await items[index].scroll_into_view_if_needed()
        await asyncio.sleep(0.3)
        item_text = await items[index].text_content()
        logging.info(f"Clicando no item '{item_text.strip()}'")
        await items[index].click()
        await asyncio.sleep(1)

async def selecionar_primeiro_item_teclado(page, container_id):
    logging.info(f"Selecionando primeiro item via teclado no dropdown {container_id}")
    try:
        await page.focus(f'div.chosen-container#{container_id} input.chosen-search-input')
    except:
        await page.focus(f'div.chosen-container#{container_id} > a')

    items = await page.query_selector_all(f'div.chosen-container#{container_id} ul.chosen-results > li')
    if items and len(items) > 0:
        first_item_text = await items[0].text_content()
        current_selection = await page.eval_on_selector(f'div.chosen-container#{container_id} a span', 'el => el.innerText')
        if current_selection and first_item_text.strip() in current_selection:
            logging.info(f"Primeiro item '{first_item_text.strip()}' já está selecionado, pressionando Enter diretamente.")
            await page.keyboard.press("Enter")
        else:
            await asyncio.sleep(0.5)
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(0.5)
            await page.keyboard.press("Enter")
        await asyncio.sleep(1)

async def limpar_pesquisa(page):
    try:
        await page.wait_for_selector('#buttonLimparPesquisarcarro a.text', state='visible', timeout=5000)
        limpar_link = page.locator('#buttonLimparPesquisarcarro a.text')
        await limpar_link.scroll_into_view_if_needed()
        await limpar_link.click()
        logging.info(">>> Pesquisa limpa com sucesso.")
        await asyncio.sleep(2)
    except Exception as e:
        logging.warning(f"[ERRO ao tentar limpar pesquisa]: {e}")
        
async def run(max_marcas=None, max_modelos=None, max_anos=None):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        try:
            logging.info("Acessando o site da FIPE...")
            await page.goto('https://veiculos.fipe.org.br/', timeout=120000)

            logging.info("Clicando em 'Consulta de Carros e Utilitários Pequenos'...")
            await page.wait_for_selector('li:has-text("Carros e utilitários pequenos")', state='visible', timeout=30000)
            await page.click('li:has-text("Carros e utilitários pequenos")')

            logging.info("Selecionando Tabela de Referência...")
            await abrir_dropdown_e_esperar(page, "selectTabelaReferenciacarro_chosen")
            await selecionar_primeiro_item_teclado(page, "selectTabelaReferenciacarro_chosen")

            logging.info("Aguardando carregamento de Marcas...")
            await abrir_dropdown_e_esperar(page, "selectMarcacarro_chosen")
            marcas = await page.query_selector_all('div.chosen-container#selectMarcacarro_chosen ul.chosen-results > li')
            max_marcas = len(marcas) if max_marcas is None else min(max_marcas, len(marcas))

            for marca_index in tqdm(range(max_marcas), desc="Marcas"):
                try:
                    nome_marca = await marcas[marca_index].text_content()
                    logging.info(f"Processando Marca [{marca_index+1}]: {nome_marca.strip()}")

                    await abrir_dropdown_e_esperar(page, "selectMarcacarro_chosen")
                    await selecionar_item_por_index(page, "selectMarcacarro_chosen", marca_index, use_arrow=True)

                    logging.info("Aguardando carregamento de Modelos...")
                    await abrir_dropdown_e_esperar(page, "selectAnoModelocarro_chosen")
                    modelos = await page.query_selector_all('div.chosen-container#selectAnoModelocarro_chosen ul.chosen-results > li')
                    max_modelos_loop = len(modelos) if max_modelos is None else min(max_modelos, len(modelos))

                    for modelo_index in range(max_modelos_loop):
                        try:
                            nome_modelo = await modelos[modelo_index].text_content()
                            logging.info(f"  Modelo [{modelo_index+1}]: {nome_modelo.strip()}")

                            await abrir_dropdown_e_esperar(page, "selectAnoModelocarro_chosen")
                            await selecionar_item_por_index(page, "selectAnoModelocarro_chosen", modelo_index, use_arrow=True)

                            logging.info("  Aguardando carregamento de Anos...")
                            await abrir_dropdown_e_esperar(page, "selectAnocarro_chosen")
                            anos = await page.query_selector_all('div.chosen-container#selectAnocarro_chosen ul.chosen-results > li')
                            max_anos_loop = len(anos) if max_anos is None else min(max_anos, len(anos))

                            for ano_index in range(max_anos_loop):
                                try:
                                    nome_ano = await anos[ano_index].text_content()
                                    logging.info(f"    Ano [{ano_index+1}]: {nome_ano.strip()}")

                                    await abrir_dropdown_e_esperar(page, "selectAnocarro_chosen")
                                    await selecionar_item_por_index(page, "selectAnocarro_chosen", ano_index, use_arrow=True)

                                    logging.info("    Realizando busca...")
                                    botao_pesquisar = page.locator('#buttonPesquisarcarro')
                                    await botao_pesquisar.scroll_into_view_if_needed()
                                    await botao_pesquisar.click(force=True)

                                    await asyncio.sleep(5)

                                    await page.wait_for_selector('div#resultadoConsultacarroFiltros td', state='visible', timeout=10000)

                                    codigo_fipe = ""
                                    preco_medio = ""

                                    tabela = await page.query_selector('table#resultadoConsultacarroFiltros')
                                    if tabela:
                                        linhas = await tabela.query_selector_all('tr')
                                        for linha in linhas:
                                            tds = await linha.query_selector_all('td')
                                            if len(tds) >= 2:
                                                texto_coluna = await tds[0].inner_text()
                                                if "Código Fipe" in texto_coluna:
                                                    codigo_fipe_element = await tds[1].query_selector('p')
                                                    codigo_fipe = await codigo_fipe_element.inner_text() if codigo_fipe_element else ""
                                                elif "Preço Médio" in texto_coluna:
                                                    preco_medio_element = await tds[1].query_selector('p')
                                                    preco_medio = await preco_medio_element.inner_text() if preco_medio_element else ""

                                    preco_medio = preco_medio.strip().replace('R$', '').replace('.', '').replace(',', '.') if preco_medio else ""

                                    logging.info(f"    Código Fipe extraído: {codigo_fipe}")
                                    logging.info(f"    Preço Médio extraído: {preco_medio}")

                                    linhas = await page.query_selector_all('table#resultadoConsultacarroFiltros tr')
                                    dados_tabela = {}
                                    ultima_label = None

                                    for linha in linhas:
                                        tds = await linha.query_selector_all('td')
                                        if len(tds) == 2:
                                            nome_element = await tds[0].query_selector('p, strong')
                                            valor_element = await tds[1].query_selector('p, strong')
                                            nome_coluna = (await nome_element.inner_text()).strip() if nome_element else (await tds[0].inner_text()).strip()
                                            valor_coluna = (await valor_element.inner_text()).strip() if valor_element else (await tds[1].inner_text()).strip()
                                            dados_tabela[nome_coluna] = valor_coluna
                                            ultima_label = nome_coluna
                                        elif len(tds) == 1:
                                            valor_element = await tds[0].query_selector('p, strong')
                                            valor_coluna = (await valor_element.inner_text()).strip() if valor_element else (await tds[0].inner_text()).strip()
                                            if ultima_label and 'noborder' in (await tds[0].get_attribute('class') or ''):
                                                dados_tabela[ultima_label] = valor_coluna

                                    dados = {
                                        "MarcaSelecionada": nome_marca.strip(),
                                        "ModeloSelecionado": nome_modelo.strip(),
                                        "AnoSelecionado": nome_ano.strip(),
                                        "CodigoFipe": codigo_fipe,
                                        "PrecoMedio": preco_medio,
                                        **dados_tabela
                                    }

                                    Fipe.append(dados)
                                    logging.info(f"    Dados salvos no Fipe: {dados}")
                                    pd.DataFrame(Fipe).to_excel("Fipe_temp.xlsx", index=False)

                                except Exception as e:
                                    logging.warning(f"[ERRO] Ano [{ano_index+1}] do Modelo [{nome_modelo.strip()}]: {e}")
                                    await asyncio.sleep(2)
                                    
                        finally:
                            # --- Limpar Pesquisa (após Modelo)
                            await limpar_pesquisa(page)

                            # Reposiciona dropdown para a Marca atual novamente
                            await abrir_dropdown_e_esperar(page, "selectMarcacarro_chosen")
                            await selecionar_item_por_index(page, "selectMarcacarro_chosen", marca_index, use_arrow=True)

                except Exception as e:
                    logging.warning(f"[ERRO] Marca [{marca_index+1}]: {e}")

        except Exception as e:
            logging.error(f"[ERRO GERAL]: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(run(max_marcas=None, max_modelos=None, max_anos=None))

    Fipe_df = pd.DataFrame(Fipe)
    print("\n\nDADOS FINAIS COLETADOS")
    print(Fipe_df)
    Fipe_df.to_excel("Fipe.xlsx", index=False)
