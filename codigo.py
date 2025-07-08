import pandas as pd
import os
import asyncio
import logging
from playwright.async_api import async_playwright

# Configura log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Caminho para os códigos FIPE
CODIGOS_XLSX = r"C:\Users\gabriel.vinicius\Documents\Códigos Fipe\Carros.xlsx"
df_cod = pd.read_excel(CODIGOS_XLSX)
lista_codigos = df_cod["codigoFipe"].dropna().astype(str).unique().tolist()
MAX_ANOS = None

# Função auxiliar para salvar dados
def salvar_temp_excel(dados):
    temp = "Fipe_temp_teste.xlsx"
    novo = pd.DataFrame([dados])
    if os.path.exists(temp):
        antigo = pd.read_excel(temp)
        df = pd.concat([antigo, novo], ignore_index=True).drop_duplicates()
    else:
        df = novo
    df.to_excel(temp, index=False)

# Seleciona a aba de pesquisa por código
async def selecionar_aba_pesquisa_por_codigo(page):
    await page.click('a[data-aba="Abacarro-codigo"]')
    await page.wait_for_selector('input[name="txtCodigoFipe"]', timeout=10000)

# Abre dropdown de ano-modelo
async def abrir_dropdown_e_esperar(page, chosen_id):
    await page.click(f'#{chosen_id}')
    await page.wait_for_selector(f'#{chosen_id} .chosen-drop li', timeout=30000)
    
# Seleciona item no dropdown com setas    
async def selecionar_item_por_index(page, container_id, index, use_arrow=False):
    logging.info(f"Dropdown {container_id} → item {index+1}")
    await abrir_dropdown_e_esperar(page, container_id)
    await page.focus(f'div.chosen-container#{container_id} > a')
    await asyncio.sleep(1)

    if use_arrow:
        await page.keyboard.press("Home")
        for _ in range(index):
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(0.15)
        await page.keyboard.press("Enter")
    else:
        itens = await page.query_selector_all(
            f'div.chosen-container#{container_id} ul.chosen-results > li')
        if index < len(itens):
            await itens[index].scroll_into_view_if_needed()
            await itens[index].click()
    await asyncio.sleep(0.8)

# Seleciona o primeiro item do dropdown
async def selecionar_primeiro_item_teclado(page, container_id):
    await abrir_dropdown_e_esperar(page, container_id)
    await page.keyboard.press("ArrowDown")
    await page.keyboard.press("Enter")
    await asyncio.sleep(0.6)

# Clica no botão para limpar a pesquisa após pegar os dados da tabela
async def limpar_pesquisa(page):
    try:
        # Aguarda o botão ficar visível
        await page.wait_for_selector("#buttonLimparPesquisarcarroPorCodigoFipe", state='visible', timeout=32000)

        # Força scroll e clica no botão
        limpar_link = page.locator('#buttonLimparPesquisarcarroPorCodigoFipe')
        await limpar_link.scroll_into_view_if_needed()
        await asyncio.sleep(0.5)
        await limpar_link.click()
        logging.info(">>> Pesquisa limpa com sucesso.")

        # Aguarda o campo de código FIPE voltar a ficar vazio
        await page.wait_for_function(
            """() => {
                const input = document.querySelector('input[name="txtCodigoFipe"]');
                return input && input.value.trim() === '';
            }""",
            timeout=30000
        )
        logging.info(">>> Confirmação visual: campo de Código FIPE resetado.")
        await asyncio.sleep(0.8)

    except Exception as e:
        logging.warning(f"[ERRO ao tentar limpar pesquisa]: {e}")

# Processa um único código FIPE
async def extracao_dados(page, cod_fipe, max_anos=None):
    await page.fill('input[name="txtCodigoFipe"]', cod_fipe)
    await abrir_dropdown_e_esperar(page, "selectCodigoAnocarroCodigoFipe_chosen")

    anos = await page.query_selector_all('div#selectCodigoAnocarroCodigoFipe_chosen ul.chosen-results > li:not(.group-result)')
    total_anos = len(anos) if max_anos is None else min(max_anos, len(anos))
    logging.info(f"[{cod_fipe}] {total_anos} ano(s) encontrados")
    
    for ano_idx in range(total_anos):
        try:
            
            await selecionar_item_por_index(page, "selectCodigoAnocarroCodigoFipe_chosen", ano_idx, use_arrow=True)
            logging.info(f">>> Coletando ano {ano_idx+1}/{total_anos} para código {cod_fipe}")
            
            # 4. Clica em pesquisar
            await page.click('#buttonPesquisarcarroPorCodigoFipe')
            await page.wait_for_selector('div#resultadocarroCodigoFipe', timeout=60000)
            
            tds = lambda lbl: f'td:has-text("{lbl}") + td p'

            codigo_fipe_elements = await page.locator('td:has-text("Código Fipe") + td p').all_text_contents()
            preco_medio_elements = await page.locator('td:has-text("Preço Médio") + td p').all_text_contents()

            codigo_fipe = next((x.strip() for x in codigo_fipe_elements if x.strip() and not x.strip().startswith('{')), "")
            preco_medio = next((x.strip().replace('R$', '').replace('.', '').replace(',', '.') for x in preco_medio_elements if x.strip() and not x.strip().startswith('{')), "")

            logging.info(f"    Código Fipe extraído: {codigo_fipe}")
            logging.info(f"    Preço Médio extraído: {preco_medio}")

            mes_referencia_elements = await page.locator('td:has-text("Mês de referência") + td p').all_text_contents()
            marca_elements = await page.locator('td:has-text("Marca") + td p').all_text_contents()
            modelo_elements = await page.locator('td:has-text("Modelo") + td p').all_text_contents()
            ano_modelo_elements = await page.locator('td:has-text("Ano Modelo") + td p').all_text_contents()

            mes_referencia = next((x.strip() for x in mes_referencia_elements if x.strip() and not x.strip().startswith('{')), "")
            marca = next((x.strip() for x in marca_elements if x.strip() and not x.strip().startswith('{')), "")
            modelo = next((x.strip() for x in modelo_elements if x.strip() and not x.strip().startswith('{')), "")
            ano_modelo = next((x.strip() for x in ano_modelo_elements if x.strip() and not x.strip().startswith('{')), "")
            
            linhas = await page.query_selector_all('div#resultadoConsultacarroCodigoFipe tr')
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
                "MarcaSelecionada": marca,
                "ModeloSelecionado": modelo,
                "AnoSelecionado": ano_modelo,
                "CodigoFipe": codigo_fipe,
                "PrecoMedio": preco_medio,
                "Mes Referencia": mes_referencia,
                **dados_tabela
            }

            logging.info(f"[OK] {codigo_fipe} - {ano_modelo}")
            salvar_temp_excel(dados)
            
        except Exception as e:
            logging.warning(f"[ERRO] Falha no ano {ano_idx+1} de {cod_fipe}: {e}")
            
        # Limpa a pesquisa para o próximo ano
        await limpar_pesquisa(page)

        # Preenche o código novamente
        await page.fill('input[name="txtCodigoFipe"]', cod_fipe)
        

# Função principal para rodar a coleta
async def run_por_codigo():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=50)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto("https://veiculos.fipe.org.br/", timeout=120000)
        await page.click('li:has-text("Carros e utilitários pequenos")')
        await selecionar_aba_pesquisa_por_codigo(page)

        for cod in lista_codigos:
            try:
                logging.info(f"Iniciando código FIPE: {cod}")
                await extracao_dados(page, cod, max_anos=MAX_ANOS)
            except Exception as e:
                logging.warning(f"Falhou no código {cod}: {e}")
                await selecionar_aba_pesquisa_por_codigo(page)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_por_codigo())