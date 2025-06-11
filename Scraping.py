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
    await asyncio.sleep(0.5)
    await page.wait_for_selector(f'div.chosen-container#{container_id} ul.chosen-results > li', state='attached', timeout=15000)

async def selecionar_primeiro_item_teclado(page, container_id):
    logging.info(f"Selecionando primeiro item via teclado no dropdown {container_id}")
    try:
        await page.focus(f'div.chosen-container#{container_id} input.chosen-search-input')
        await asyncio.sleep(0.3)
        await page.keyboard.press("ArrowDown")
        await asyncio.sleep(0.3)
        await page.keyboard.press("Enter")
        await asyncio.sleep(0.5)
    except:
        logging.info(f"Campo de busca não disponível em {container_id}, usando seta + enter no botão principal.")
        await page.focus(f'div.chosen-container#{container_id} > a')
        await page.keyboard.press("ArrowDown")
        await asyncio.sleep(0.3)
        await page.keyboard.press("Enter")
        await asyncio.sleep(0.5)

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
                    await selecionar_primeiro_item_teclado(page, "selectMarcacarro_chosen")

                    logging.info("Aguardando carregamento de Modelos...")
                    await abrir_dropdown_e_esperar(page, "selectAnoModelocarro_chosen")
                    modelos = await page.query_selector_all('div.chosen-container#selectAnoModelocarro_chosen ul.chosen-results > li')
                    max_modelos_loop = len(modelos) if max_modelos is None else min(max_modelos, len(modelos))

                    for modelo_index in range(max_modelos_loop):
                        try:
                            nome_modelo = await modelos[modelo_index].text_content()
                            logging.info(f"  Modelo [{modelo_index+1}]: {nome_modelo.strip()}")

                            await abrir_dropdown_e_esperar(page, "selectAnoModelocarro_chosen")
                            await selecionar_primeiro_item_teclado(page, "selectAnoModelocarro_chosen")

                            logging.info("  Aguardando carregamento de Anos...")
                            await abrir_dropdown_e_esperar(page, "selectAnocarro_chosen")
                            anos = await page.query_selector_all('div.chosen-container#selectAnocarro_chosen ul.chosen-results > li')
                            max_anos_loop = len(anos) if max_anos is None else min(max_anos, len(anos))

                            for ano_index in range(max_anos_loop):
                                try:
                                    nome_ano = await anos[ano_index].text_content()
                                    logging.info(f"    Ano [{ano_index+1}]: {nome_ano.strip()}")

                                    await abrir_dropdown_e_esperar(page, "selectAnocarro_chosen")
                                    await selecionar_primeiro_item_teclado(page, "selectAnocarro_chosen")

                                    logging.info("    Realizando busca...")
                                    botao_pesquisar = page.locator('#buttonPesquisarcarro')
                                    await botao_pesquisar.scroll_into_view_if_needed()
                                    await botao_pesquisar.click(force=True)

                                    for tentativa in range(3):
                                        logging.info(f"    Tentativa {tentativa+1}/3 para carregar resultado...")
                                        tabela_carregada = False

                                        max_tentativas_internas = 10
                                        for tentativa_interna in range(max_tentativas_internas):
                                            try:
                                                await page.wait_for_function("""
                                                    () => {
                                                        const div = document.querySelector('div#resultadoConsultacarroFiltros');
                                                        return div && window.getComputedStyle(div).display !== 'none';
                                                    }
                                                """, timeout=2000)
                                                logging.info(f"    Tabela carregada com sucesso na tentativa interna {tentativa_interna+1}!")
                                                tabela_carregada = True
                                                break
                                            except:
                                                logging.info(f"    Tentativa interna {tentativa_interna+1}/{max_tentativas_internas}: tabela ainda não carregada...")
                                                await asyncio.sleep(2)

                                        if tabela_carregada:
                                            break
                                        else:
                                            logging.warning("    Resultado não carregado, tentando clicar em PESQUISAR novamente...")
                                            await botao_pesquisar.click(force=True)
                                            await asyncio.sleep(2)

                                    linhas = await page.query_selector_all('table#resultadoConsultacarroFiltros tr')
                                    dados_tabela = {}
                                    for linha in linhas:
                                        tds = await linha.query_selector_all('td')
                                        if len(tds) >= 2:
                                            nome_element = await tds[0].query_selector('p')
                                            nome_coluna = await nome_element.inner_text() if nome_element else (await tds[0].inner_text())

                                            valor_element = await tds[1].query_selector('p')
                                            valor_coluna = await valor_element.inner_text() if valor_element else (await tds[1].inner_text())

                                            dados_tabela[nome_coluna.strip()] = valor_coluna.strip()

                                    logging.info(f"    Tabela completa coletada: {dados_tabela}")

                                    dados = {
                                        "MarcaSelecionada": nome_marca.strip(),
                                        "ModeloSelecionado": nome_modelo.strip(),
                                        "AnoSelecionado": nome_ano.strip(),
                                        **dados_tabela
                                    }

                                    Fipe.append(dados)
                                    logging.info(f"    Dados salvos no Fipe: {dados}")

                                    pd.DataFrame(Fipe).to_excel("Fipe_temp.xlsx", index=False)

                                except Exception as e:
                                    logging.warning(f"[ERRO] Ano [{ano_index+1}] do Modelo [{nome_modelo.strip()}]: {e}")
                                    await asyncio.sleep(2)
                        except Exception as e:
                            logging.warning(f"[ERRO] Modelo [{modelo_index+1}] da Marca [{nome_marca.strip()}]: {e}")
                            await asyncio.sleep(2)
                except Exception as e:
                    logging.warning(f"[ERRO] Marca [{marca_index+1}]: {e}")

        except Exception as e:
            logging.error(f"[ERRO GERAL]: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(run(max_marcas=3, max_modelos=2, max_anos=2))

    Fipe_df = pd.DataFrame(Fipe)
    print("\n\nDADOS FINAIS COLETADOS")
    print(Fipe_df)
    Fipe_df.to_excel("Fipe.xlsx", index=False)
