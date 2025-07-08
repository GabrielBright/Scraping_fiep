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
MES = "meses_processados_carros.json"

# Cria json se não houver Log de Marcas
if not os.path.exists("marcas_processadas.json"):
    with open("marcas_processadas.json", "w") as f:
        json.dump([], f)

# Garante que o arquivo existe
if not os.path.exists(JSON):
    with open(JSON, "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=2)

# Carrega modelos já processados
with open(JSON, "r", encoding="utf-8") as f:
    modelos_processados = json.load(f)
    
if not os.path.exists("meses_processados_carros.json"):
    with open("meses_processados_carros.json"):
        json.dump([], f)
        
# Garante que o arquivo existe
if not os.path.exists(MES):
    with open(MES, "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=2)

# Carrega meses já processados
with open(MES, "r", encoding="utf-8") as f:
    meses_processados = json.load(f)
    
# Serve para carregar os meses pegos do arquivo JSON
def carregar_meses_processados():
    try:
        with open(MES, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"Erro ao carregar modelos processados: {e}")
        return {}

# Salva após coletar todos os meses de rferencia
def salvar_meses_processados(meses_processados):
    with open(MES, "w", encoding="utf-8") as f:
        json.dump(meses_processados, f, ensure_ascii=False, indent=2)

# Carrega as Marcas do Json
def carregar_marcas_processadas():
    try:
        with open("marcas_processadas.json", "r") as f:
            return set(json.load(f))
    except Exception as e:
        logging.warning(f"Não foi possivel carregar as marcas processadas {e}")
        return set()

# Salva as marcas no json
def salvar_marcas_processadas(marcas_processadas):
    with open("marcas_processadas.json", "w") as f:
        json.dump(list(marcas_processadas), f)

# Serve para carregar os modelos pegos do arquivo JSON
def carregar_modelos_processados():
    try:
        with open("modelos_processados_carros.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"Erro ao carregar modelos processados: {e}")
        return {}

# Salva após coletar todos os anos modelos de determinado modelo
def salvar_modelos_processados(modelos_processados):
    with open("modelos_processados_carros.json", "w", encoding="utf-8") as f:
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

# Clica no botão para limpar a pesquisa após pegar os dados da tabela
async def limpar_pesquisa(page):
    try:
        await page.wait_for_selector('#buttonLimparPesquisarcarro a.text', state='visible', timeout=5000)
        limpar_link = page.locator('#buttonLimparPesquisarcarro a.text')
        await limpar_link.scroll_into_view_if_needed()
        await limpar_link.click()
        logging.info(">>> Pesquisa limpa com sucesso.")
        await asyncio.sleep(2)

        await page.wait_for_function(
            """() => {
                const span = document.querySelector('#selectMarcacarro_chosen a span');
                return span && span.textContent.toLowerCase().includes('selecione');
            }""",
            timeout=10000
        )
        logging.info(">>> Confirmação visual: dropdown de Marca resetado.")
    except Exception as e:
        logging.warning(f"[ERRO ao tentar limpar pesquisa]: {e}")

# Função deita para facilitar e limpar o código
async def fechar_todos_dropdowns(page):
    await page.keyboard.press("Escape")
    await asyncio.sleep(0.3)
    await page.evaluate("document.activeElement.blur();")
    await asyncio.sleep(0.3)

def split_lotes(total_marcas, n_lotes=3):
    return [list(range(i, total_marcas, n_lotes)) for i in range(n_lotes)]

async def obter_modelos_disponiveis(page):
    await abrir_dropdown_e_esperar(page, "selectAnoModelocarro_chosen")
    modelos = await page.query_selector_all('div.chosen-container#selectAnoModelocarro_chosen ul.chosen-results > li')
    modelos_nomes = [ (await m.text_content()).strip() for m in modelos ]
    return modelos, modelos_nomes

# Função para processar marca, ou seja, como cada aba vai processar a coleta de dados
async def processar_marca(page, marca_index, marcas_nomes, modelos_processados, marcas_processadas, max_modelos, max_anos, nome_mes):
    
    nome_marca = marcas_nomes[marca_index]
    logging.info(f"Processando Marca [{marca_index+1}]: {nome_marca}")

    try:
        await page.goto('https://veiculos.fipe.org.br/', timeout=120000)

        await page.wait_for_selector('li:has-text("Carros e utilitários pequenos")', timeout=60000)
        await page.click('li:has-text("Carros e utilitários pequenos")')
        
        await abrir_dropdown_e_esperar(page, "selectTabelaReferenciacarro_chosen")
        meses_dropdown = await page.query_selector_all('div.chosen-container#selectTabelaReferenciacarro_chosen ul.chosen-results > li')
        nomes_meses = [await m.text_content() for m in meses_dropdown]
        nomes_meses = [m.strip() for m in nomes_meses]

        if nome_mes not in nomes_meses:
            logging.error(f"[ERRO] Mês '{nome_mes}' não encontrado no dropdown de Tabela de Referência!")
            return

        indice_mes = nomes_meses.index(nome_mes)
        await selecionar_item_por_index(page, "selectTabelaReferenciacarro_chosen", indice_mes, use_arrow=True)
        
        await abrir_dropdown_e_esperar(page, "selectMarcacarro_chosen")
        await selecionar_item_por_index(page, "selectMarcacarro_chosen", marca_index, use_arrow=True)
        logging.info("Aguardando carregamento de Modelos...")
        await abrir_dropdown_e_esperar(page, "selectAnoModelocarro_chosen")
        
        modelos, modelos_nomes = await obter_modelos_disponiveis(page)
        max_modelos_loop = len(modelos_nomes) if max_modelos is None else min(max_modelos, len(modelos_nomes))

        # Verificação rápida: se o último modelo já está no JSON, considera tudo processado
        ultimo_modelo_disponivel = modelos_nomes[max_modelos_loop - 1] if max_modelos_loop > 0 else None
        modelos_ja_processados = modelos_processados.get(nome_marca, [])

        if ultimo_modelo_disponivel and ultimo_modelo_disponivel in modelos_ja_processados:
            logging.info(f"[SKIP] Todos os modelos da marca {nome_marca} já foram processados. Pulando...")
            marcas_processadas.add(nome_marca.strip())
            salvar_marcas_processadas(marcas_processadas)
            return
        
        if nome_marca not in modelos_processados:
            modelos_processados[nome_marca] = []
        
        modelos_ja_processados = modelos_processados.get(nome_marca, [])

        indice_modelo_inicial = 0
        if modelos_processados.get(nome_marca):
            ultimo_modelo_salvo = modelos_processados[nome_marca][-1]
            if ultimo_modelo_salvo in modelos_nomes:
                indice_modelo_inicial = modelos_nomes.index(ultimo_modelo_salvo) + 1
                logging.info(f"[RETOMADA] Continuando do modelo '{ultimo_modelo_salvo}' (índice {indice_modelo_inicial})")

        # Determina ponto de retomada para modelos da marca atual
        for modelo_index in range(indice_modelo_inicial, max_modelos_loop):
            try:
                await abrir_dropdown_e_esperar(page, "selectAnoModelocarro_chosen")
                modelos, modelos_nomes = await obter_modelos_disponiveis(page)

                # Se o dropdown não tem esse índice, espera mais e tenta de novo
                tentativas_dropdown = 0
                while modelo_index >= len(modelos_nomes):
                    logging.warning(f"[AVISO] Dropdown de modelos ainda incompleto (esperado índice {modelo_index}, mas só há {len(modelos_nomes)}).")
                    await asyncio.sleep(5)
                    await abrir_dropdown_e_esperar(page, "selectAnoModelocarro_chosen")
                    modelos, modelos_nomes = await obter_modelos_disponiveis(page)
                    tentativas_dropdown += 1
                    if tentativas_dropdown >= 3:
                        logging.error(f"[FALHA] Dropdown de modelos não carregou corretamente após 3 tentativas. Recarregando página.")
                        await page.reload(wait_until="domcontentloaded")
                        await asyncio.sleep(5)
                        try:
                            await page.wait_for_selector('div#selectMarcacarro_chosen', timeout=15000)
                            await abrir_dropdown_e_esperar(page, "selectMarcacarro_chosen")
                            logging.info("✔ Página recarregada e dropdown de Marca disponível.")
                        except Exception as e:
                            logging.warning(f"[ERRO após reload] Dropdown da marca não apareceu: {e}")
                            return
                        await processar_marca(page, marca_index, marcas_nomes, modelos_processados, marcas_processadas, max_modelos, max_anos, nome_mes)
                        return  # Sai dessa execução para recomeçar com a marca do zero

                nome_modelo = modelos_nomes[modelo_index]
                sucesso_todos_anos = True
                
                logging.info(f"  Modelo [{modelo_index+1}]: {nome_modelo}")
                await abrir_dropdown_e_esperar(page, "selectAnoModelocarro_chosen")
                await selecionar_item_por_index(page, "selectAnoModelocarro_chosen", modelo_index, use_arrow=True)
                await page.wait_for_selector('#buttonPesquisarcarro', state='visible', timeout=60000)

                # Verifica se o modelo foi realmente selecionado
                modelo_selecionado = await page.locator('#selectAnoModelocarro_chosen span').inner_text()
                if nome_modelo not in modelo_selecionado:
                    logging.warning(f"[AVISO] Falha ao selecionar o modelo {nome_modelo}, tentando resetar dropdowns...")
                    # Refaz a seleção completa
                    await abrir_dropdown_e_esperar(page, "selectMarcacarro_chosen")
                    await selecionar_item_por_index(page, "selectMarcacarro_chosen", marca_index, use_arrow=True)
                    await page.keyboard.press("Escape")
                    await page.wait_for_selector('#buttonPesquisarcarro', state='visible', timeout=70000)

                    await abrir_dropdown_e_esperar(page, "selectAnoModelocarro_chosen")
                    await selecionar_item_por_index(page, "selectAnoModelocarro_chosen", modelo_index, use_arrow=True)
                    await page.keyboard.press("Escape")
                    await page.wait_for_selector('#buttonPesquisarcarro', state='visible', timeout=70000)

                await abrir_dropdown_e_esperar(page, "selectAnocarro_chosen")
                anos = await page.query_selector_all('div.chosen-container#selectAnocarro_chosen ul.chosen-results > li')
                max_anos_loop = len(anos) if max_anos is None else min(max_anos, len(anos))

                for ano_index in range(max_anos_loop):
                    try:
                        await limpar_pesquisa(page)
                        await page.wait_for_selector('#buttonPesquisarcarro', state='visible', timeout=65000)

                        if ano_index > 0:
                            await abrir_dropdown_e_esperar(page, "selectMarcacarro_chosen")
                            await selecionar_item_por_index(page, "selectMarcacarro_chosen", marca_index, use_arrow=True)
                            await page.keyboard.press("Escape")
                            await page.wait_for_selector('#buttonPesquisarcarro', state='visible', timeout=65000)

                            await abrir_dropdown_e_esperar(page, "selectAnoModelocarro_chosen")
                            await selecionar_item_por_index(page, "selectAnoModelocarro_chosen", modelo_index, use_arrow=True)
                            await page.keyboard.press("Escape")
                            await page.wait_for_selector('#buttonPesquisarcarro', state='visible', timeout=55000)

                        nome_ano = await anos[ano_index].text_content()
                        logging.info(f"    Ano [{ano_index+1}]: {nome_ano.strip()}")

                        await abrir_dropdown_e_esperar(page, "selectAnocarro_chosen")
                        await selecionar_item_por_index(page, "selectAnocarro_chosen", ano_index, use_arrow=True)

                        logging.info("    Realizando busca...")
                        botao_pesquisar = page.locator('#buttonPesquisarcarro')
                        await botao_pesquisar.scroll_into_view_if_needed()
                        await botao_pesquisar.click(force=True)

                        await page.wait_for_selector('#buttonPesquisarcarro', state='visible', timeout=55000)
                        await page.wait_for_selector('div#resultadoConsultacarroFiltros', state='visible', timeout=55000)

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
                            "MarcaSelecionada": marca,
                            "ModeloSelecionado": modelo,
                            "AnoSelecionado": ano_modelo,
                            "CodigoFipe": codigo_fipe,
                            "PrecoMedio": preco_medio,
                            "Mes Referencia": mes_referencia,
                            **dados_tabela
                        }

                        logging.info(f"Dados salvos no Fipe: {dados}")

                        temp = "Fipe_temp.xlsx"
                        fipe_temp_novo = pd.DataFrame([dados])
                        if os.path.exists(temp):
                            fipe_temp_antigo = pd.read_excel(temp)
                            df_completo = pd.concat([fipe_temp_antigo, fipe_temp_novo], ignore_index=True)
                            df_completo = df_completo.drop_duplicates()
                        else:
                            df_completo = fipe_temp_novo

                        df_completo.to_excel(temp, index=False)

                    except Exception as e:
                        logging.warning(f"[ERRO] Ano [{ano_index+1}] do Modelo [{nome_modelo.strip()}]: {e}")
                        sucesso_todos_anos = False
                        await asyncio.sleep(3)
                        break
                        
            # Se deu tudo certo com todos os anos, grava o modelo no JSON
                if sucesso_todos_anos:
                    if nome_marca not in modelos_processados:
                        modelos_processados[nome_marca] = []
                    if nome_modelo not in modelos_processados[nome_marca]:
                        modelos_processados[nome_marca].append(nome_modelo)
                        salvar_modelos_processados(modelos_processados)
                        logging.info(f"[OK] Modelo {nome_modelo} finalizado e gravado.")
                else:
                    logging.info(f"[RETOMAR] Modelo {nome_modelo} ficou incompleto; será reprocessado depois.")

            except Exception as e:
                logging.warning(f"[ERRO] Modelo [{modelo_index+1}]: {e}")
                await asyncio.sleep(3)
                
        # Loga que terminou a marca e quantos modelos foram processados
        logging.info(f"[CONCLUÍDO] Marca {nome_marca}: {len(modelos_processados[nome_marca])} modelos processados.")

    except Exception as e:
        logging.warning(f"[ERRO] Marca [{marca_index+1}]: {e}")
        await asyncio.sleep(3)
    
    finally:
        marcas_processadas.add(nome_marca.strip())
        salvar_marcas_processadas(marcas_processadas)
        
async def processar_lote(playwright, indices, marcas_lista, nome_mes, modelos_processados, marcas_processadas, max_modelos, max_anos):
    browser = await playwright.chromium.launch(headless=False)
    context = await browser.new_context()
    queue = Queue()

    for idx in indices:
        await queue.put(idx)

    tasks = [
        asyncio.create_task(
            worker(queue, context, marcas_lista, modelos_processados, marcas_processadas, max_modelos, max_anos, nome_mes)
            )
        ]

    await queue.join()
    await asyncio.gather(*tasks, return_exceptions=True)
    await context.close()
    await browser.close()

async def processar_lote_com_contexto(context, indices, marcas_lista, nome_mes, modelos_processados, marcas_processadas, max_modelos, max_anos):
    queue = Queue()

    for idx in indices:
        await queue.put(idx)

    tasks = [
        asyncio.create_task(
            worker(queue, context, marcas_lista, modelos_processados, marcas_processadas, max_modelos, max_anos, nome_mes)
        )
    ]

    await queue.join()
    await asyncio.gather(*tasks, return_exceptions=True)
    
# Para conseguir processar em multiplas abas o SCRAPING
async def worker(queue, context, marcas_lista, modelos_processados, marcas_processadas, max_modelos, max_anos, nome_mes):
    while not queue.empty():
        index = await queue.get()
        page = await context.new_page()
        try:
            logging.info(f"[Worker] Processando: {marcas_lista[index]} ({index})")
            await processar_marca(page, index, marcas_lista, modelos_processados, marcas_processadas, max_modelos, max_anos, nome_mes)
        except Exception as e:
            logging.error(f"[Worker-Erro] Marca {index}: {e}")
        finally:
            await page.close()
            queue.task_done()

# Função principal modificada para processar 3 marcas em paralelo
async def run(max_marcas=None, max_modelos=None, max_anos=None):
    modelos_processados = carregar_modelos_processados()
    marcas_processadas = carregar_marcas_processadas()
    meses_processados = carregar_meses_processados()

    async with async_playwright() as p:
        browser_temp = await p.chromium.launch(headless=False)
        context_temp = await browser_temp.new_context()
        page = await context_temp.new_page()

        try:
            logging.info("Acessando a página principal para capturar meses e marcas...")
            await page.goto('https://veiculos.fipe.org.br/', timeout=120000)
            await page.click('li:has-text("Carros e utilitários pequenos")')
            await abrir_dropdown_e_esperar(page, "selectTabelaReferenciacarro_chosen")
            meses_dropdown = await page.query_selector_all('div.chosen-container#selectTabelaReferenciacarro_chosen ul.chosen-results > li')
            nomes_meses = [await m.text_content() for m in meses_dropdown]
            nomes_meses = [m.strip() for m in nomes_meses]

            await abrir_dropdown_e_esperar(page, "selectMarcacarro_chosen")
            marcas = await page.query_selector_all('div.chosen-container#selectMarcacarro_chosen ul.chosen-results > li')
            marcas_lista = [await m.text_content() for m in marcas]
            marcas_lista = [m.strip() for m in marcas_lista]

            logging.info(f"[INFO] {len(marcas_lista)} marcas capturadas.")
        finally:
            await page.close()
            await context_temp.close()
            await browser_temp.close()

        total_marcas = len(marcas_lista) if max_marcas is None else min(max_marcas, len(marcas_lista))
        lotes = [list(range(i, total_marcas, 3)) for i in range(3)]

        for nome_mes in nomes_meses:
            if meses_processados.get(nome_mes):
                logging.info(f"[PULANDO] Mês já processado: {nome_mes}")
                continue

            logging.info(f"\n▶ INICIANDO MÊS: {nome_mes} com 3 navegadores em sequência...")

           # Cria 3 navegadores independentes com seus próprios contextos
            browser_1 = await p.chromium.launch(headless=False)
            browser_2 = await p.chromium.launch(headless=False)
            browser_3 = await p.chromium.launch(headless=False)

            context_1 = await browser_1.new_context()
            context_2 = await browser_2.new_context()
            context_3 = await browser_3.new_context()

            # Roda os 3 lotes em paralelo
            await asyncio.gather(
                processar_lote_com_contexto(context_1, lotes[0], marcas_lista, nome_mes, modelos_processados, marcas_processadas, max_modelos, max_anos),
                processar_lote_com_contexto(context_2, lotes[1], marcas_lista, nome_mes, modelos_processados, marcas_processadas, max_modelos, max_anos),
                processar_lote_com_contexto(context_3, lotes[2], marcas_lista, nome_mes, modelos_processados, marcas_processadas, max_modelos, max_anos)
            )

            # Fecha os navegadores
            await context_1.close()
            await browser_1.close()
            await context_2.close()
            await browser_2.close()
            await context_3.close()
            await browser_3.close()

            meses_processados[nome_mes] = True
            salvar_meses_processados(meses_processados)

if __name__ == "__main__":
    asyncio.run(run(max_marcas=None, max_modelos=None, max_anos=None))
    
    Fipe_df = pd.DataFrame(Fipe)
    print("\n\nDADOS FINAIS COLETADOS")
    print(Fipe_df)
    Fipe_df.to_excel("Fipe.xlsx", index=False)