import pandas as pd
import os
import asyncio
import sys
import logging
from tqdm import tqdm
from playwright.async_api import async_playwright

sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

Fipe = []

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        await page.goto('https://veiculos.fipe.org.br/', timeout=60000)

        # CLICA na faixa "Consulta de Carros e Utilitários Pequenos"
        print("Clicando em 'Consulta de Carros e Utilitários Pequenos'...")
        await page.wait_for_selector('//*[@id="front"]/div[1]/div[2]/ul/li[1]', timeout=60000)
        await page.click('//*[@id="front"]/div[1]/div[2]/ul/li[1]')

        # Espera o formulário carregar
        print("Esperando formulário carregar...")
        await page.wait_for_selector('//form[@class="form"]', timeout=10000)

        # Seleciona tabela de referência mais recente
        print("Selecionando Tabela de Referência...")
        await page.wait_for_selector('//*[@id="selectTabelaReferencial_chosen"]/a', timeout=60000)
        await page.click('//*[@id="selectTabelaReferencial_chosen"]/a')
        await page.click('//*[@id="selectTabelaReferencial_chosen"]/div/ul/li[1]')

        # Aguarda carregar marcas
        print("Aguardando carregamento de Marcas...")
        await page.wait_for_selector('//*[@id="selectMarcacarro_chosen"]/a')
        await page.click('//*[@id="selectMarcacarro_chosen"]/a')
        await page.wait_for_selector('//*[@id="selectMarcacarro_chosen"]//li[contains(@class, "active-result")]', timeout=10000)
        marcas = await page.query_selector_all('//*[@id="selectMarcacarro_chosen"]//li[contains(@class, "active-result")]')

        # Loop de marcas com barra de progresso
        for marca_index in tqdm(range(1, min(3, len(marcas))), desc="Marcas"):
            try:
                nome_marca = await marcas[marca_index].text_content()
                print(f"\n--> Marca [{marca_index}]: {nome_marca.strip()}")

                await page.click('//*[@id="selectMarcacarro_chosen"]/a')
                await page.wait_for_selector('//*[@id="selectMarcacarro_chosen"]//li[contains(@class, "active-result")]', timeout=10000)
                marcas = await page.query_selector_all('//*[@id="selectMarcacarro_chosen"]//li[contains(@class, "active-result")]')
                await marcas[marca_index].click()

                # Aguarda carregar modelos
                print("    Aguardando carregamento de Modelos...")
                await page.wait_for_selector('//*[@id="selectAnoModelocarro_chosen"]/a')
                await page.click('//*[@id="selectAnoModelocarro_chosen"]/a')
                await page.wait_for_selector('//*[@id="selectAnoModelocarro_chosen"]//li[contains(@class, "active-result")]', timeout=10000)
                modelos = await page.query_selector_all('//*[@id="selectAnoModelocarro_chosen"]//li[contains(@class, "active-result")]')

                for modelo_index in range(1, min(2, len(modelos))):
                    try:
                        nome_modelo = await modelos[modelo_index].text_content()
                        print(f"    --> Modelo [{modelo_index}]: {nome_modelo.strip()}")

                        await page.click('//*[@id="selectAnoModelocarro_chosen"]/a')
                        await page.wait_for_selector('//*[@id="selectAnoModelocarro_chosen"]//li[contains(@class, "active-result")]', timeout=10000)
                        modelos = await page.query_selector_all('//*[@id="selectAnoModelocarro_chosen"]//li[contains(@class, "active-result")]')
                        await modelos[modelo_index].click()

                        # Aguarda carregar anos
                        print("        Aguardando carregamento de Anos...")
                        await page.wait_for_selector('//*[@id="selectAnocarro_chosen"]/a')
                        await page.click('//*[@id="selectAnocarro_chosen"]/a')
                        await page.wait_for_selector('//*[@id="selectAnocarro_chosen"]//li[contains(@class, "active-result")]', timeout=10000)
                        anos = await page.query_selector_all('//*[@id="selectAnocarro_chosen"]//li[contains(@class, "active-result")]')

                        for ano_index in range(1, min(2, len(anos))):
                            try:
                                nome_ano = await anos[ano_index].text_content()
                                print(f"        --> Ano [{ano_index}]: {nome_ano.strip()}")

                                await page.click('//*[@id="selectAnocarro_chosen"]/a')
                                await page.wait_for_selector('//*[@id="selectAnocarro_chosen"]//li[contains(@class, "active-result")]', timeout=10000)
                                anos = await page.query_selector_all('//*[@id="selectAnocarro_chosen"]//li[contains(@class, "active-result")]')
                                await anos[ano_index].click()

                                # Scroll até o botão e clica
                                print("            Realizando busca no botão Pesquisar...")
                                botao_pesquisar = page.locator('//*[@id="front"]/div[1]/div[2]/ul/li[1]/div/article[1]/div[3]/div[3]')
                                await botao_pesquisar.scroll_into_view_if_needed()
                                await botao_pesquisar.click()

                                # Aguarda resultado
                                print("            Aguardando resultado...")
                                await page.wait_for_selector('//*[@id="resultadoConsultacarroFiltros"]', timeout=120000)

                                # Extrai os campos
                                mes_referencia = await page.inner_text('//*[@id="resultadoConsultacarroFiltros"]/table/tbody/tr[1]/td[2]/p')
                                codigo_fipe    = await page.inner_text('//*[@id="resultadoConsultacarroFiltros"]/table/tbody/tr[2]/td[2]/p')
                                marca_res      = await page.inner_text('//*[@id="resultadoConsultacarroFiltros"]/table/tbody/tr[3]/td[2]/p')
                                modelo_res     = await page.inner_text('//*[@id="resultadoConsultacarroFiltros"]/table/tbody/tr[4]/td[2]/p')
                                ano_modelo_res = await page.inner_text('//*[@id="resultadoConsultacarroFiltros"]/table/tbody/tr[5]/td[2]/p')
                                preco_medio    = await page.inner_text('//*[@id="resultadoConsultacarroFiltros"]/table/tbody/tr[8]/td[2]/p')

                                # Adiciona ao DataFrame
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

                                print(f"            -> Dados coletados: {dados}")
                                Fipe.append(dados)

                            except Exception as e:
                                logging.warning(f"[ERRO] Ano [{ano_index}] do Modelo [{nome_modelo.strip()}]: {e}")
                    except Exception as e:
                        logging.warning(f"[ERRO] Modelo [{modelo_index}] da Marca [{nome_marca.strip()}]: {e}")
            except Exception as e:
                logging.warning(f"[ERRO] Marca [{marca_index}]: {e}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())

    Fipe_df = pd.DataFrame(Fipe)
    print("\n\nDADOS FINAIS COLETADOS")
    print(Fipe_df)
    Fipe_df.to_excel("Fipe.xlsx", index=False)
