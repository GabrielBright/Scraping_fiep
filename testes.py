import pandas as pd
import os
import asyncio
import sys
import logging
import re
from tqdm import tqdm
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import json 
from asyncio import Queue
import random

# Configura encoding e logging
sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

Fipe = []

JSON = "modelos_processados_motos.json"
CATALOGO_JSON = "catalogo_modelos_motos.json"

# Cria json se não houver Log de Marcas
if not os.path.exists("marcas_processadas_motos.json"):
    with open("marcas_processadas_motos.json", "w") as f:
        json.dump([], f)

catalogo_modelos_raw = {}
if os.path.exists(CATALOGO_JSON):
    try:
        with open(CATALOGO_JSON, "r", encoding="utf-8") as f:
            catalogo_modelos_raw = json.load(f)
    except Exception as e:
        logging.error(f"[CATÁLOGO] Erro lendo {CATALOGO_JSON}: {e}")
else:
    logging.error(f"[CATÁLOGO] Arquivo {CATALOGO_JSON} não encontrado.")

if isinstance(catalogo_modelos_raw, dict) and "dados" in catalogo_modelos_raw:
    catalogo_modelos_raw = catalogo_modelos_raw["dados"]

def _norm(s: str) -> str:
    return (s or "").strip().lower()

catalogo_modelos = {}
if isinstance(catalogo_modelos_raw, dict):
    for marca, modelos in catalogo_modelos_raw.items():
        norm_marca = _norm(marca)
        if isinstance(modelos, list):
            catalogo_modelos[norm_marca] = sorted({m.strip() for m in modelos if (m or "").strip()})
        else:
            logging.warning(f"[CATÁLOGO] Modelos da marca '{marca}' não é lista. Ignorando.")
else:
    logging.error("[CATÁLOGO] Estrutura inválida no JSON de catálogo.")

def obter_modelos_da_marca(catalogo: dict, nome_marca: str):
    norm = (nome_marca or "").strip().lower()
    if norm in catalogo:
        return catalogo[norm]
    for k in catalogo.keys():
        if norm in k or k in norm:
            return catalogo[k]
    return []

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
        with open("marcas_processadas_motos.json", "r") as f:
            return set(json.load(f))
    except Exception as e:
        logging.warning(f"Não foi possivel carregar as marcas processadas {e}")
        return set()

# Salva as marcas no json
def salvar_marcas_processadas(marcas_processadas):
    with open("marcas_processadas_motos.json", "w") as f:
        json.dump(list(marcas_processadas),f)

def carregar_modelos_processados():
    try:
        with open("modelos_processados_motos.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"Erro ao carregar modelos processados: {e}")
        return {}
    
def salvar_modelos_processados(modelos_processados):
    with open("modelos_processados_motos.json", "w", encoding="utf-8") as f:
        json.dump(modelos_processados, f, ensure_ascii=False, indent=2)

async def pausa_curta():
    await asyncio.sleep(random.uniform(0.8, 1.6))

async def pausa_lenta():
    delay = random.uniform(8, 12)  
    logging.info(f"Pausa anti-bloqueio: {delay:.1f}s")
    await asyncio.sleep(delay)
        
# Abre o dropdown/Seleção de itens e deixa aberto um tempo para carregar
async def abrir_dropdown_e_esperar(page, container_id):
    logging.info(f"Abrindo dropdown: {container_id}")
    # Clica no elemento <a class="chosen-single">
    await page.click(f'div.chosen-container#{container_id} a.chosen-single', force=True)
    await asyncio.sleep(0.5)
    # Espera lista aparecer
    await page.wait_for_selector(f'div.chosen-container#{container_id} .chosen-drop ul.chosen-results > li', state='visible', timeout=5000)
    
def dividir_em_lotes(marcas_lista, max_workers):
    tamanho_lote = (len(marcas_lista) + max_workers - 1) // max_workers
    return [marcas_lista[i*tamanho_lote:(i+1)*tamanho_lote] for i in range(max_workers)]

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
        
async def selecionar_marca_por_nome(page, nome_marca):
    logging.info(f"Selecionando marca pelo nome: {nome_marca}")

    # Fecha qualquer dropdown ativo antes
    await page.keyboard.press("Escape")
    await asyncio.sleep(0.3)

    # Abre especificamente o dropdown de MARCA
    await page.click("#selectMarcamoto_chosen a.chosen-single", force=True)

    # Garante que o dropdown certo está ativo
    await page.wait_for_function(
        """() => document.querySelector('#selectMarcamoto_chosen.chosen-container-active') !== null""",
        timeout=3000
    )

    # Usa seletor fixo do input correto
    marca_input = "#selectMarcamoto_chosen > div.chosen-drop > div.chosen-search > input[type='text']"
    await page.wait_for_selector(marca_input, state="visible")
    await page.fill(marca_input, nome_marca)
    await asyncio.sleep(0.5)

    # Aguarda carregar os itens filtrados
    await page.wait_for_selector("div#selectMarcamoto_chosen ul.chosen-results li", timeout=5000)
    itens = await page.query_selector_all("div#selectMarcamoto_chosen ul.chosen-results li")
    for item in itens:
        texto = (await item.inner_text()).strip()
        if texto.lower() == nome_marca.lower():
            await item.click()
            break

async def selecionar_modelo_por_texto(page, nome_modelo, tentativas=3):
    for tentativa in range(1, tentativas+1):
        try:
            # abre dropdown de modelos
            await abrir_dropdown_e_esperar(page, "selectAnoModelomoto_chosen")
            # input de busca dentro do chosen:
            input_sel = "#selectAnoModelomoto_chosen .chosen-search input[type='text']"
            await page.wait_for_selector(input_sel, state="visible", timeout=5000)
            # limpa e digita
            await page.fill(input_sel, "")
            await asyncio.sleep(0.2)
            await page.fill(input_sel, nome_modelo)
            await asyncio.sleep(0.6)

            # pega itens filtrados
            itens = await page.query_selector_all("div#selectAnoModelomoto_chosen ul.chosen-results li")
            if not itens:
                logging.warning(f"[Modelo] Nenhum item apareceu ao filtrar '{nome_modelo}' (tentativa {tentativa}).")
            else:
                # clica no primeiro que contenha o texto (case-insensitive)
                clicou = False
                alvo_lower = nome_modelo.lower()
                for item in itens:
                    txt = (await item.inner_text() or "").strip()
                    if alvo_lower in txt.lower():
                        await item.click()
                        clicou = True
                        break
                if not clicou:
                    # fallback: clica no primeiro item listado
                    await itens[0].click()

                await asyncio.sleep(0.5)
                selecionado = (await page.locator('#selectAnoModelomoto_chosen span').inner_text()).strip()
                if nome_modelo.lower() in selecionado.lower():
                    return True

            # se falhou, fecha, pausa e tenta de novo
            await page.keyboard.press("Escape")
            await pausa_curta()
        except Exception as e:
            logging.warning(f"[Modelo] Erro ao selecionar '{nome_modelo}' (tentativa {tentativa}): {e}")
            await pausa_curta()
    return False

# A primeira vez que abre alguma seleção escolhe o primeiro item do dropdown
async def selecionar_primeiro_item_teclado(page, container_id):
    logging.info(f"Selecionando primeiro item via teclado no dropdown {container_id}")
    try:
        await page.focus(f'div.chosen-container#{container_id}.chosen-container-active input.chosen-search-input')
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
async def limpar_pesquisa(page, nome_marca=None):
    try:
        # Clica no botão limpar
        await page.wait_for_selector('#buttonLimparPesquisarmoto a.text', state='visible', timeout=5000)
        limpar_link = page.locator('#buttonLimparPesquisarmoto a.text')
        await limpar_link.scroll_into_view_if_needed()
        await limpar_link.click()
        logging.info(">>> Pesquisa limpa com sucesso.")
        await asyncio.sleep(2)

        # Aguarda reset visual
        await page.wait_for_function(
            """() => {
                const span = document.querySelector('#selectMarcamoto_chosen a span');
                return span && span.textContent.toLowerCase().includes('selecione');
            }""",
            timeout=10000
        )
        logging.info(">>> Confirmação visual: dropdown de Marca resetado.")

        # Se nome de marca foi passado, re-seleciona
        if nome_marca:
            await selecionar_marca_por_nome(page, nome_marca)

    except Exception as e:
        logging.warning(f"[ERRO ao tentar limpar pesquisa]: {e}")

async def fechar_todos_dropdowns(page):
    await page.keyboard.press("Escape")
    await asyncio.sleep(0.3)
    await page.evaluate("document.activeElement.blur();")
    await asyncio.sleep(0.3)
    
async def worker(queue, browser, marcas_lista, modelos_processados, marcas_processadas, max_modelos, max_anos):
    # cada worker cria seu contexto isolado
    context = await browser.new_context()
    page = await context.new_page()

    await page.goto('https://veiculos.fipe.org.br/', timeout=120000)
    await page.wait_for_selector('li:has-text("Motos")', timeout=30000)
    await page.click('li:has-text("Motos")')

    while not queue.empty():
        marca_index = await queue.get()
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
            queue.task_done()

    await page.close()
    await context.close()

async def obter_modelos_disponiveis(page):
    await abrir_dropdown_e_esperar(page, "selectAnoModelomoto_chosen")
    modelos = await page.query_selector_all('div.chosen-container#selectAnoModelomoto_chosen ul.chosen-results > li')
    modelos_nomes = [ (await m.text_content()).strip() for m in modelos ]
    return modelos, modelos_nomes

# Função para processar marca, ou seja, como cada aba vai processar a coleta de dados
async def processar_marca(page, marca_index, marcas_nomes, modelos_processados, marcas_processadas, max_modelos, max_anos):
    
    nome_marca = marcas_nomes[marca_index]
    logging.info(f"Processando Marca [{marca_index+1}]: {nome_marca}")

    try:
        await abrir_dropdown_e_esperar(page, "selectMarcamoto_chosen")
        await selecionar_marca_por_nome(page, nome_marca)
        await pausa_curta()
        logging.info("Aguardando carregamento de Modelos...")

        # pega do catálogo (case-insensitive/frouxo)
        modelos_da_marca = obter_modelos_da_marca(catalogo_modelos, nome_marca)

        if not modelos_da_marca:
            logging.warning(f"[CATÁLOGO] Marca '{nome_marca}' não encontrada no catálogo. Tentando ler do dropdown como fallback...")
            try:
                await abrir_dropdown_e_esperar(page, "selectAnoModelomoto_chosen")
                itens = await page.query_selector_all('div.chosen-container#selectAnoModelomoto_chosen ul.chosen-results > li')
                modelos_da_marca = []
                for it in itens:
                    t = (await it.text_content() or "").strip()
                    if t:
                        modelos_da_marca.append(t)
                modelos_da_marca = sorted({m for m in modelos_da_marca})
                logging.info(f"[FALLBACK] Dropdown retornou {len(modelos_da_marca)} modelos para '{nome_marca}'.")
            except Exception as e:
                logging.warning(f"[FALLBACK] Falhou capturar modelos via dropdown para '{nome_marca}': {e}")

        if not modelos_da_marca:
            logging.warning(f"[SEM MODELOS] Marca '{nome_marca}' ficou sem modelos mesmo após fallback. Pulando...")
            marcas_processadas.add(nome_marca.strip())
            salvar_marcas_processadas(marcas_processadas)
            return

        # resume: se já processou alguns, retoma do próximo
        indice_modelo_inicial = 0
        if modelos_processados.get(nome_marca):
            ultimo_modelo_salvo = modelos_processados[nome_marca][-1]
            if ultimo_modelo_salvo in modelos_da_marca:
                indice_modelo_inicial = modelos_da_marca.index(ultimo_modelo_salvo) + 1
                logging.info(f"[RETOMADA] Continuando do modelo '{ultimo_modelo_salvo}' (índice {indice_modelo_inicial})")

        total_modelos = len(modelos_da_marca)
        logging.info(f"Total de modelos (catálogo) para {nome_marca}: {total_modelos}")

        # Loop pelos modelos do catálogo
        for modelo_index in range(indice_modelo_inicial, total_modelos):
            try:
                nome_modelo = modelos_da_marca[modelo_index].strip()
                if not nome_modelo: 
                    continue
                logging.info(f" Modelo [{modelo_index+1}/{total_modelos}]: {nome_modelo}")

                # re-seleciona a marca SEMPRE pra garantir estado limpo
                await abrir_dropdown_e_esperar(page, "selectMarcamoto_chosen")
                await selecionar_marca_por_nome(page, nome_marca)
                await pausa_curta()

                # seleciona o modelo pelo texto (com retries)
                ok_modelo = await selecionar_modelo_por_texto(page, nome_modelo, tentativas=3)
                if not ok_modelo:
                    logging.warning(f"[SKIP] Não consegui selecionar o modelo '{nome_modelo}'. Pulando...")
                    continue

                await page.wait_for_selector('#buttonPesquisarmoto', state='visible', timeout=50000)

                # abre anos
                await abrir_dropdown_e_esperar(page, "selectAnomoto_chosen")
                await pausa_curta()
                anos = await page.query_selector_all('div.chosen-container#selectAnomoto_chosen ul.chosen-results > li')
                anos_lista = [(idx, (await ano.text_content()).strip()) for idx, ano in enumerate(anos)]

                if not anos_lista:
                    logging.warning(f"[SKIP] Modelo '{nome_modelo}' não possui ano-modelo. Pulando...")
                    await page.keyboard.press("Escape")
                    await asyncio.sleep(0.5)
                    continue

                max_anos_loop = len(anos_lista) if max_anos is None else min(max_anos, len(anos_lista))
                for ano_idx_loop in range(max_anos_loop):
                    ano_index, nome_ano = anos_lista[ano_idx_loop]
                    try:
                        # limpeza e re-seleção da marca e modelo apenas quando muda o ano
                        await pausa_curta()
                        await limpar_pesquisa(page, nome_marca)
                        await pausa_curta()
                        await page.wait_for_selector('#buttonPesquisarmoto', state='visible', timeout=15000)

                        if ano_index > 0:
                            await abrir_dropdown_e_esperar(page, "selectMarcamoto_chosen")
                            await selecionar_marca_por_nome(page, nome_marca)
                            await pausa_curta()

                            ok_modelo = await selecionar_modelo_por_texto(page, nome_modelo, tentativas=2)
                            if not ok_modelo:
                                logging.warning(f"[ANO] Falhei re-selecionar '{nome_modelo}' para ano '{nome_ano}'. Pulando ano.")
                                continue
                            await page.wait_for_selector('#buttonPesquisarmoto', state='visible', timeout=30000)

                        logging.info(f"    Ano [{ano_index+1}]: {nome_ano.strip()}")

                        await abrir_dropdown_e_esperar(page, "selectAnomoto_chosen")
                        await selecionar_item_por_index(page, "selectAnomoto_chosen", ano_index, use_arrow=True)
                        await pausa_curta()

                        # buscar
                        botao_pesquisar = page.locator('#buttonPesquisarmoto')
                        await botao_pesquisar.scroll_into_view_if_needed()
                        await botao_pesquisar.click(force=True)
                        await pausa_curta()

                        await page.wait_for_selector('#buttonPesquisarmoto', state='visible', timeout=50000)
                        await page.wait_for_selector('div#resultadoConsultamotoFiltros', state='visible', timeout=30000)

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

                        linhas = await page.query_selector_all('table#resultadoConsultamotoFiltros tr')
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

                        temp = "Fipe_temp_motos.xlsx"
                        fipe_temp_novo = pd.DataFrame([dados])
                        if os.path.exists(temp):
                            fipe_temp_antigo = pd.read_excel(temp)
                            df_completo = pd.concat([fipe_temp_antigo, fipe_temp_novo], ignore_index=True)
                            df_completo = df_completo.drop_duplicates()
                        else:
                            df_completo = fipe_temp_novo
                        
                        # após salvar
                        if nome_marca not in modelos_processados:
                            modelos_processados[nome_marca] = []
                        if nome_modelo not in modelos_processados[nome_marca]:
                            modelos_processados[nome_marca].append(nome_modelo)
                            salvar_modelos_processados(modelos_processados)

                        await pausa_lenta()

                    except Exception as e:
                        logging.warning(f"[ERRO] Ano [{ano_index+1}] do Modelo [{nome_modelo}]: {e}")
                        await asyncio.sleep(2)

            except Exception as e:
                logging.warning(f"[ERRO] Modelo [{modelo_index+1}]: {e}")
                await asyncio.sleep(2)
                
        # Loga que terminou a marca e quantos modelos foram processados
        logging.info(f"[CONCLUÍDO] Marca {nome_marca}: {len(modelos_processados[nome_marca])} modelos processados.")
        await pausa_lenta()

    except Exception as e:
        logging.warning(f"[ERRO] Marca [{marca_index+1}]: {e}")
        await asyncio.sleep(2)
    
    finally:
        marcas_processadas.add(nome_marca.strip())
        salvar_marcas_processadas(marcas_processadas)

# Função principal modificada para processar 3 marcas em paralelo
async def run(max_marcas=None, max_modelos=None, max_anos=None, max_workers=2):
    marcas_processadas = carregar_marcas_processadas()
    modelos_processados = carregar_modelos_processados()

    # Lê marcas do Excel (já salvas antes)
    df_marcas = pd.read_excel("marcas.xlsx")
    marcas_lista = df_marcas["Marca"].tolist()
    logging.info(f"Total de marcas carregadas do Excel: {len(marcas_lista)}")

    if max_marcas:
        marcas_lista = marcas_lista[:max_marcas]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

        lotes = dividir_em_lotes(list(range(len(marcas_lista))), max_workers)
        tasks = []
        for lote in lotes:
            queue = Queue()
            for marca_index in lote:
                await queue.put(marca_index)
            tasks.append(asyncio.create_task(worker(queue, browser, marcas_lista, modelos_processados, marcas_processadas, max_modelos, max_anos)))

        await asyncio.gather(*tasks)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(run(max_marcas=None, max_modelos=None, max_anos=None))
    
    Fipe_df = pd.DataFrame(Fipe)
    print("\n\nDADOS FINAIS COLETADOS")
    print(Fipe_df)
    Fipe_df.to_excel("Fipe_moto.xlsx", index=False)