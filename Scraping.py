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
    """Força o dropdown do Chosen a abrir corretamente."""
    logging.info(f"Abrindo dropdown: {container_id}")
    await page.focus(f'div.chosen-container#{container_id} > a')
    await page.click(f'div.chosen-container#{container_id} > a')
    await asyncio.sleep(0.5)

    # Ativa o campo de input se existir (existe no de Marca!)
    try:
        await page.fill(f'div.chosen-container#{container_id} input.chosen-search-input', '')
        await asyncio.sleep(0.5)
    except Exception:
        pass  # Se não tiver input, segue normal (ex: dropdown de Anos não tem input)

    # Espera o dropdown abrir de fato
    await page.wait_for_selector(f'div.chosen-container#{container_id} ul.chosen-results > li', state='attached', timeout=25000)

async def clicar_dropdown_item(page, container_id):
    """Clica diretamente no primeiro item ativo do dropdown, sem hover, para evitar fechar o dropdown com scroll."""
    logging.info(f"Clicando no item de {container_id}")

    # Espera ter pelo menos um item .active-result
    await page.wait_for_selector(f'div.chosen-container#{container_id} ul.chosen-results > li.active-result', timeout=6000)

    locator = page.locator(f'div.chosen-container#{container_id} ul.chosen-results > li.active-result')
    await locator.first.click()


async def run(max_marcas=None, max_modelos=None, max_anos=None):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        try:
            logging.info("Acessando o site da FIPE...")
            await page.goto('https://veiculos.fipe.org.br/', timeout=120000)

            # Clica em "Consulta de Carros e Utilitários Pequenos"
            logging.info("Clicando em 'Consulta de Carros e Utilitários Pequenos'...")
            await page.wait_for_selector('li:has-text("Carros e utilitários pequenos")', state='visible', timeout=60000)
            await page.click('li:has-text("Carros e utilitários pequenos")')

            # Seleciona tabela de referência
            logging.info("Selecionando Tabela de Referência...")
            await abrir_dropdown_e_esperar(page, "selectTabelaReferenciacarro_chosen")
            await clicar_dropdown_item(page, "selectTabelaReferenciacarro_chosen")

            # Aguarda e coleta marcas
            logging.info("Aguardando carregamento de Marcas...")
            await abrir_dropdown_e_esperar(page, "selectMarcacarro_chosen")
            marcas = await page.query_selector_all('div.chosen-container#selectMarcacarro_chosen ul.chosen-results > li')
            max_marcas = len(marcas) if max_marcas is None else min(max_marcas, len(marcas))

            for marca_index in tqdm(range(max_marcas), desc="Marcas"):
                try:
                    nome_marca = await marcas[marca_index].text_content()
                    logging.info(f"Processando Marca [{marca_index+1}]: {nome_marca.strip()}")

                    await abrir_dropdown_e_esperar(page, "selectMarcacarro_chosen")
                    await clicar_dropdown_item(page, "selectMarcacarro_chosen")

                    # Aguarda carregar modelos
                    logging.info("Aguardando carregamento de Modelos...")
                    await abrir_dropdown_e_esperar(page, "selectAnoModelocarro_chosen")
                    modelos = await page.query_selector_all('div.chosen-container#selectAnoModelocarro_chosen ul.chosen-results > li')
                    max_modelos_loop = len(modelos) if max_modelos is None else min(max_modelos, len(modelos))

                    for modelo_index in range(max_modelos_loop):
                        try:
                            nome_modelo = await modelos[modelo_index].text_content()
                            logging.info(f"  Modelo [{modelo_index+1}]: {nome_modelo.strip()}")

                            await abrir_dropdown_e_esperar(page, "selectAnoModelocarro_chosen")
                            await clicar_dropdown_item(page, "selectAnoModelocarro_chosen")

                            # Aguarda carregar anos
                            logging.info("  Aguardando carregamento de Anos...")
                            await abrir_dropdown_e_esperar(page, "selectAnocarro_chosen")
                            anos = await page.query_selector_all('div.chosen-container#selectAnocarro_chosen ul.chosen-results > li')
                            max_anos_loop = len(anos) if max_anos is None else min(max_anos, len(anos))

                            for ano_index in range(max_anos_loop):
                                try:
                                    nome_ano = await anos[ano_index].text_content()
                                    logging.info(f"    Ano [{ano_index+1}]: {nome_ano.strip()}")

                                    await abrir_dropdown_e_esperar(page, "selectAnocarro_chosen")
                                    await clicar_dropdown_item(page, "selectAnocarro_chosen")

                                    # Clica no botão Pesquisar
                                    logging.info("    Realizando busca...")
                                    botao_pesquisar = page.locator('#buttonPesquisarcarro')
                                    await botao_pesquisar.scroll_into_view_if_needed()
                                    await botao_pesquisar.click(force=True)

                                    # Aguarda resultado
                                    logging.info("    Aguardando resultado da pesquisa...")

                                    # Aqui esperamos um <p> DENTRO da tabela, para garantir que os dados já renderizaram
                                    await page.wait_for_selector('table#resultadoConsultacarroFiltros p', state='attached', timeout=60000)

                                    # Agora podemos extrair os dados
                                    mes_referencia = await page.inner_text('table#resultadoConsultacarroFiltros tr:nth-child(1) td:nth-child(2) p')
                                    codigo_fipe    = await page.inner_text('table#resultadoConsultacarroFiltros tr:nth-child(2) td:nth-child(2) p')
                                    marca_res      = await page.inner_text('table#resultadoConsultacarroFiltros tr:nth-child(3) td:nth-child(2) p')
                                    modelo_res     = await page.inner_text('table#resultadoConsultacarroFiltros tr:nth-child(4) td:nth-child(2) p')
                                    ano_modelo_res = await page.inner_text('table#resultadoConsultacarroFiltros tr:nth-child(5) td:nth-child(2) p')

                                    # Nem sempre a linha 8 (Preço Médio) existe! Fazemos um try/except
                                    try:
                                        preco_medio = await page.inner_text('table#resultadoConsultacarroFiltros tr:nth-child(8) td:nth-child(2) p')
                                    except:
                                        preco_medio = "N/A"

                                    dados = {
                                        "MarcaSelecionada": nome_marca.strip(),
                                        "ModeloSelecionado": nome_modelo.strip(),
                                        "AnoSelecionado": nome_ano.strip(),
                                        "MesReferencia": mes_referencia.strip(),
                                        "CodigoFipe": codigo_fipe.strip(),
                                        "MarcaResultado": marca_res.strip(),
                                        "ModeloResultado": modelo_res.strip(),
                                        "AnoModeloResultado": ano_modelo_res.strip(),
                                        "PrecoMedio": preco_medio.strip()
                                    }
                                    Fipe.append(dados)
                                    logging.info(f"    Dados coletados: {dados}")

                                    # Salva incrementalmente
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
