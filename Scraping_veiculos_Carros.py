import pandas as pd
import os
import asyncio
import sys
import logging
from tqdm import tqdm
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import json 
from asyncio import Queue

# Configura encoding e logging
sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

Fipe = []

JSON = "modelos_processados_carros.json"

# Cria json se não houver Log de Marcas
if not os.path.exists("marcas_processadas_carros.json"):
    with open("marcas_processadas_carros.json", "w") as f:
        json.dump([], f)

# Garante que o arquivo existe
if not os.path.exists(JSON):
    with open(JSON, "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=2)

# Carrega modelos já processados
with open(JSON, "r", encoding="utf-8") as f:
    modelos_processados = json.load(f)
    
# Carrega as Marcas do Json
def carregar_marcas_processadas():
    try:
        with open("marcas_processadas_caminhoes.json", "r") as f:
            return set(json.load(f))
    except Exception as e:
        logging.warning(f"Não foi possivel carregar as marcas processadas {e}")
        return set()

# Salva as marcas no json
def salvar_marcas_processadas(marcas_processadas):
    with open("marcas_processadas_caminhoes.json", "w") as f:
        json.dump(list(marcas_processadas),f)

def carregar_modelos_processados():
    try:
        with open("modelos_processados_caminhoes.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"Erro ao carregar modelos processados: {e}")
        return {}
    
def salvar_modelos_processados(modelos_processados):
    with open("modelos_processados_caminhoes.json", "w", encoding="utf-8") as f:
        json.dump(modelos_processados, f, ensure_ascii=False, indent=2)
        
# Abre o dropdown/Seleção de itens e deixa aberto um tempo para carregar
async def abrir_dropdown_e_esperar(page, container_id):
    logging.info(f"Abrindo dropdown: {container_id}")
    await page.focus(f'div.chosen-container#{container_id} > a')
    await page.click(f'div.chosen-container#{container_id} > a')
    await asyncio.sleep(2)
    await page.wait_for_selector(f'div.chosen-container#{container_id} ul.chosen-results > li', state='attached', timeout=20000)

# Seleciona o item pelo index dele
async def selecionar_item_por_index(page, container_id, index, use_arrow=False):
    logging.info(f"Selecionando item {index+1} no dropdown {container_id}")
    await abrir_dropdown_e_esperar(page, container_id)
    await page.focus(f'div.chosen-container#{container_id} > a')
    await asyncio.sleep(0.5)

    if use_arrow:

        # Fecha qualquer dropdown aberto
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.6)
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.6)
        
        # Reabre o dropdown para navegação ao índice desejado
        await abrir_dropdown_e_esperar(page, container_id)
        await page.focus(f'div.chosen-container#{container_id} > a')
        await asyncio.sleep(0.3)
        
        ultimo_texto = ""
        tentativas = 0
        max_tentativas = 30
        
        while tentativas < max_tentativas:
            itens = await page.query_selector_all(f'div.chosen-container#{container_id} ul.chosen-results > li.highlighted')
            if itens:
                texto_atual = await itens[0].text_content()
                texto_atual = texto_atual.strip() if texto_atual else ""
                if texto_atual == ultimo_texto:
                    break
                ultimo_texto = texto_atual
            
            await page.keyboard.press("ArrowUp")
            await asyncio.sleep(0.05)
            tentativas += 1  
        
        # Navega até o indice que eu quero
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

# A primeira vez que abre alguma seleção escolhe o primeiro item do dropdown
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

# Clica no botão para limpar a pesquisa apos pegar os dados da tabela
async def limpar_pesquisa(page):
    try:
        await page.wait_for_selector('#buttonLimparPesquisarcaminhao a.text', state='visible', timeout=5000)
        limpar_link = page.locator('#buttonLimparPesquisarcaminhao a.text')
        await limpar_link.scroll_into_view_if_needed()
        await limpar_link.click()
        logging.info(">>> Pesquisa limpa com sucesso.")
        await asyncio.sleep(2)

        await page.wait_for_function(
            """() => {
                const span = document.querySelector('#selectMarcacaminhao_chosen a span');
                return span && span.textContent.toLowerCase().includes('selecione');
            }""",
            timeout=10000
        )
        logging.info(">>> Confirmação visual: dropdown de Marca resetado.")
    except Exception as e:
        logging.warning(f"[ERRO ao tentar limpar pesquisa]: {e}")

async def fechar_todos_dropdowns(page):
    await page.keyboard.press("Escape")
    await asyncio.sleep(0.3)
    await page.evaluate("document.activeElement.blur();")
    await asyncio.sleep(0.3)

async def worker(queue, browser_context, marcas_lista, modelos_processados, marcas_processadas, max_modelos, max_anos):
    while not queue.empty():
        marca_index = await queue.get()
        page = await browser_context.new_page()
        try:
            await processar_marca(
                page,
                marca_index,
                marcas_lista,
                modelos_processados,
                marcas_processadas,
                max_modelos,
                max_anos
            )
        except Exception as e:
            logging.error(f"[ERRO NO WORKER - Marca {marca_index+1}]: {e}")
        finally:
            await page.close()
            queue.task_done()

# Função para processar uma única marca
async def processar_marca(page, marca_index, marcas_nomes, modelos_processados, marcas_processadas, max_modelos, max_anos):
    
    nome_marca = marcas_nomes[marca_index]
    logging.info(f"Processando Marca [{marca_index+1}]: {nome_marca}")

    try:
        await page.goto('https://veiculos.fipe.org.br/', timeout=120000)

        await page.wait_for_selector('li:has-text("Consulta de Caminhões e Micro-Ônibus")', timeout=30000)
        await page.click('li:has-text("Consulta de Caminhões e Micro-Ônibus")')

        await abrir_dropdown_e_esperar(page, "selectTabelaReferenciacaminhao_chosen")
        await selecionar_primeiro_item_teclado(page, "selectTabelaReferenciacaminhao_chosen")

        
        await abrir_dropdown_e_esperar(page, "selectMarcacaminhao_chosen")
        await selecionar_item_por_index(page, "selectMarcacaminhao_chosen", marca_index, use_arrow=True)

        logging.info("Aguardando carregamento de Modelos...")
        await abrir_dropdown_e_esperar(page, "selectAnoModelocaminhao_chosen")
        modelos = await page.query_selector_all('div.chosen-container#selectAnoModelocaminhao_chosen ul.chosen-results > li')
        max_modelos_loop = len(modelos) if max_modelos is None else min(max_modelos, len(modelos))
        
        if nome_marca not in modelos_processados:
            modelos_processados[nome_marca] = []
                    
        # Determina ponto de retomada para modelos da marca atual
        modelos_ja_processados = modelos_processados.get(nome_marca, [])
        indice_modelo_inicial = 0

        for i, modelo in enumerate(modelos):
            nome = (await modelo.text_content()).strip()
            if modelos_ja_processados and nome == modelos_ja_processados[-1]:
                indice_modelo_inicial = i + 1
                break

        # Loop principal de modelos com retomada
        for modelo_index in range(indice_modelo_inicial, max_modelos_loop):
            try:
                nome_modelo = (await modelos[modelo_index].text_content()).strip()
                if nome_modelo in modelos_processados[nome_marca]:
                    logging.info(f"  [SKIP] Modelo já processado: {nome_modelo}")
                    continue

                logging.info(f"  Modelo [{modelo_index+1}]: {nome_modelo}")

                await abrir_dropdown_e_esperar(page, "selectAnoModelocaminhao_chosen")
                await selecionar_item_por_index(page, "selectAnoModelocaminhao_chosen", modelo_index, use_arrow=True)

                await abrir_dropdown_e_esperar(page, "selectAnocaminhao_chosen")
                anos = await page.query_selector_all('div.chosen-container#selectAnocaminhao_chosen ul.chosen-results > li')
                max_anos_loop = len(anos) if max_anos is None else min(max_anos, len(anos))

                for ano_index in range(max_anos_loop):
                    try:
                        await limpar_pesquisa(page)
                        await asyncio.sleep(1.5)

                        if ano_index > 0:
                            await abrir_dropdown_e_esperar(page, "selectMarcacaminhao_chosen")
                            await selecionar_item_por_index(page, "selectMarcacaminhao_chosen", marca_index, use_arrow=True)
                            await page.keyboard.press("Escape")
                            await asyncio.sleep(0.3)

                            await abrir_dropdown_e_esperar(page, "selectAnoModelocaminhao_chosen")
                            await selecionar_item_por_index(page, "selectAnoModelocaminhao_chosen", modelo_index, use_arrow=True)
                            await page.keyboard.press("Escape")
                            await asyncio.sleep(0.3)

                        nome_ano = await anos[ano_index].text_content()
                        logging.info(f"    Ano [{ano_index+1}]: {nome_ano.strip()}")

                        await abrir_dropdown_e_esperar(page, "selectAnocaminhao_chosen")
                        await selecionar_item_por_index(page, "selectAnocaminhao_chosen", ano_index, use_arrow=True)

                        logging.info("    Realizando busca...")
                        botao_pesquisar = page.locator('#buttonPesquisarcaminhao')
                        await botao_pesquisar.scroll_into_view_if_needed()
                        await botao_pesquisar.click(force=True)

                        await asyncio.sleep(5)
                        await page.wait_for_selector('div#resultadoConsultacaminhaoFiltros', state='visible', timeout=30000)

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

                        linhas = await page.query_selector_all('table#resultadoConsultacaminhaoFiltros tr')
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

                        logging.info(f"Dados salvos no Fipe: {dados}")

                        temp = "Fipe_temp_caminhao.xlsx"
                        fipe_temp_novo = pd.DataFrame([dados])
                        if os.path.exists(temp):
                            fipe_temp_antigo = pd.read_excel(temp)
                            df_completo = pd.concat([fipe_temp_antigo, fipe_temp_novo], ignore_index=True)
                            df_completo = df_completo.drop_duplicates()
                        else:
                            df_completo = fipe_temp_novo
                        
                        if nome_marca not in modelos_processados:
                            modelos_processados[nome_marca] = []
                        
                        if nome_modelo not in modelos_processados[nome_marca]:
                            modelos_processados[nome_marca].append(nome_modelo)
                            salvar_modelos_processados(modelos_processados)

                        df_completo.to_excel(temp, index=False)

                    except Exception as e:
                        logging.warning(f"[ERRO] Ano [{ano_index+1}] do Modelo [{nome_modelo.strip()}]: {e}")
                        await asyncio.sleep(2)

            except Exception as e:
                logging.warning(f"[ERRO] Modelo [{modelo_index+1}]: {e}")
                await asyncio.sleep(2)

            finally:
                await limpar_pesquisa(page)
                await abrir_dropdown_e_esperar(page, "selectMarcacaminhao_chosen")
                await selecionar_item_por_index(page, "selectMarcacaminhao_chosen", marca_index, use_arrow=True)
                marcas_processadas.add(nome_marca.strip())
                salvar_marcas_processadas(marcas_processadas)

    except Exception as e:
        logging.warning(f"[ERRO] Marca [{marca_index+1}]: {e}")
        await asyncio.sleep(2)
    
    marcas_processadas.add(nome_marca.strip())
    salvar_marcas_processadas(marcas_processadas)

# Função principal modificada para processar 3 marcas em paralelo
async def run(max_marcas=None, max_modelos=None, max_anos=None, max_workers=5):
    marcas_processadas = carregar_marcas_processadas()
    modelos_processados = carregar_modelos_processados()

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context()

        try:
            # Abre página inicial só para pegar as marcas
            page = await context.new_page()
            logging.info("Acessando o site da FIPE...")
            await page.goto('https://veiculos.fipe.org.br/', timeout=120000)
            await page.wait_for_selector('li:has-text("Caminhões e Micro-Ônibus")', timeout=30000)
            await page.click('li:has-text("Caminhões e Micro-Ônibus")')
            logging.info("Selecionando Tabela de Referência...")
            await abrir_dropdown_e_esperar(page, "selectTabelaReferenciacaminhao_chosen")
            await selecionar_primeiro_item_teclado(page, "selectTabelaReferenciacaminhao_chosen")

            logging.info("Aguardando carregamento de Marcas...")
            await abrir_dropdown_e_esperar(page, "selectMarcacaminhao_chosen")
            marcas = await page.query_selector_all('div.chosen-container#selectMarcacaminhao_chosen ul.chosen-results > li')
            marcas_lista = [await m.text_content() for m in marcas]
            marcas_lista = [m.strip() for m in marcas_lista]
            await page.close()

            logging.warning(f"[VERIFICAÇÃO] Total de marcas mapeadas: {len(marcas_lista)}")
            for i, nome in enumerate(marcas_lista):
                logging.warning(f"Marca: [{i+1}]: {nome}")

            max_marcas = len(marcas_lista) if max_marcas is None else min(max_marcas, len(marcas_lista))

            # Fila dinâmica com as marcas
            queue = Queue()
            for i in range(max_marcas):
                await queue.put(i)

            # Cria os workers (navegadores paralelos)
            tasks = [
                asyncio.create_task(worker(queue, context, marcas_lista, modelos_processados, marcas_processadas, max_modelos, max_anos))
                for _ in range(max_workers)
            ]

            await queue.join()  # Espera todos os workers finalizarem a fila
            for task in tasks:
                task.cancel()  # Cancela workers depois da fila esvaziar

        except Exception as e:
            logging.error(f"[ERRO GERAL]: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(run(max_marcas=None, max_modelos=None, max_anos=None))

    Fipe_df = pd.DataFrame(Fipe)
    print("\n\nDADOS FINAIS COLETADOS")
    print(Fipe_df)
    Fipe_df.to_excel("Fipe_caminhao.xlsx", index=False)