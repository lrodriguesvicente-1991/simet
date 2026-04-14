# =====================================================================
# ARQUIVO: robots/lfp.py
# MÓDULO: LFP (Localizador de Fontes Primárias)
# =====================================================================

import os
import json
import re
import random
import time
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

from database.connection import obter_conexao

try:
    from playwright_stealth import stealth_sync
    aplicar_stealth = stealth_sync
except ImportError:
    aplicar_stealth = None

load_dotenv()

def obter_tarefas_ativas():
    conn = obter_conexao()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.cfg_id, u.unf_sigla, c.cfg_fonte_nome, c.cfg_url_busca, c.cfg_paginas_max 
        FROM public.smt_config_unf_scraping c
        JOIN public.smt_unidade_federativa u ON c.cfg_unf_id = u.unf_id
        WHERE c.cfg_ativo = true;
    """)
    tarefas = cursor.fetchall()
    cursor.close()
    conn.close()
    return tarefas

def salvar_na_fila(conn, fonte, url, dados_json):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO public.smt_fila_processamento (fil_fonte, fil_url, fil_conteudo_jsonb, fil_status)
        VALUES (%s, %s, %s, 'PENDENTE') ON CONFLICT (fil_url) DO NOTHING;
    """, (fonte, url, json.dumps(dados_json, ensure_ascii=False)))
    inserido = cursor.rowcount > 0
    conn.commit()
    cursor.close()
    return inserido

def extrair_olx(page):
    conteudo_html = page.content()
    if "Ops! Nenhum anuncio foi encontrado" in conteudo_html or "Ops! Nenhum resultado" in conteudo_html:
        return [] 
        
    try: 
        page.wait_for_selector('a[data-testid="ad-card-link"], a[data-ds-component="DS-NewAdCard-Link"]', timeout=10000)
    except: 
        pass
    
    ads = []
    links = page.locator('a[data-testid="ad-card-link"], a[data-ds-component="DS-NewAdCard-Link"]').all()
    
    if not links:
        area_busca = page.locator('main') if page.locator('main').count() > 0 else page.locator('body')
        links = area_busca.locator('a').all()
    
    for link in links:
        try:
            url = link.get_attribute('href')
            if url and "olx.com.br" in url and re.search(r'\d{8,}', url):
                url_limpa = url.split('?')[0] 
                texto = link.inner_text().strip()
                titulo = texto.split('\n')[0] if texto else "Anuncio OLX"
                ads.append({"titulo": titulo, "url": url_limpa, "raw": []})
        except: 
            continue
            
    return list({ad['url']: ad for ad in ads}.values())

def extrair_mfrural(page):
    try: 
        page.wait_for_selector("a[href*='/detalhe/']", timeout=15000)
    except: 
        pass 
        
    ads = []
    for link in page.locator("a").all():
        try:
            url = link.get_attribute('href')
            if not url: continue
            if url.startswith('/'): 
                url = "https://www.mfrural.com.br" + url
            if "/detalhe/" in url:
                texto = link.inner_text().strip()
                if texto: 
                    ads.append({"titulo": texto.split('\n')[0], "url": url, "raw": texto.split('\n')})
        except: 
            continue
            
    return list({ad['url']: ad for ad in ads}.values())

def executar_lfp(tarefas, evento_fim=None):
    if not tarefas: 
        return
    
    print("[LFP] Iniciando motores de rede...", flush=True)
    conn = obter_conexao()
    total_geral_novos = 0
    modo_headless = os.getenv("SIMET_HEADLESS", "0") == "1"
    modo_nav = os.getenv("SIMET_NAVEGACAO", "RAPIDA")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="chrome",
            headless=modo_headless, 
            args=["--disable-blink-features=AutomationControlled", "--disable-infobars", "--no-sandbox"]
        )
        context = browser.new_context(
            viewport={'width': 1366, 'height': 768},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        if aplicar_stealth: aplicar_stealth(page)
        
        for t in tarefas:
            cfg_id, sigla, fonte, url_busca, p_max = t
            pag = 1
            erros_consecutivos = 0
            paginas_sem_novos = 0  
            links_da_pagina_anterior = set() 
            
            while True:
                if p_max > 0 and pag > p_max: break
                if erros_consecutivos >= 5: 
                    print(f"[LFP] Limite de exceções excedido em {sigla}. Abortando.", flush=True)
                    break
                
                limite_str = str(p_max) if p_max > 0 else "∞"
                # A String abaixo manteve a estrutura de pipes "|" para o Frontend poder quebrar e ler sem os emojis.
                print(f"[LFP] Varredura iniciada no estado {sigla} | Plataforma: {fonte} | Pág: {pag}/{limite_str}", flush=True)
                
                url_p = url_busca if pag == 1 else (f"{url_busca}{'&' if '?' in url_busca else '?'}o={pag}" if fonte.upper() == 'OLX' else f"{url_busca}?pg={pag}")
                wait_mode = "domcontentloaded" if fonte.upper() == 'OLX' else "networkidle"
                
                try:
                    page.goto(url_p, timeout=60000, wait_until=wait_mode)
                    time.sleep(random.uniform(2, 4))
                    
                    for _ in range(8):
                        page.keyboard.press("PageDown")
                        time.sleep(random.uniform(0.5, 1.2))
                    
                    ads = extrair_olx(page) if fonte.upper() == 'OLX' else extrair_mfrural(page)
                    links_da_pagina_atual = {ad['url'] for ad in ads}
                    
                    if not links_da_pagina_atual or links_da_pagina_atual == links_da_pagina_anterior:
                        print(f"[LFP] Fim da paginação em {sigla}.", flush=True)
                        break
                        
                    erros_consecutivos = 0
                    novos = 0
                    for ad in ads:
                        if salvar_na_fila(conn, fonte, ad['url'], ad): 
                            novos += 1
                            total_geral_novos += 1
                            
                    if novos == 0: paginas_sem_novos += 1
                    else: paginas_sem_novos = 0 
                        
                    print(f"[LFP] Leitura concluída: {sigla} (Pág {pag}) | Amostras: {len(ads)} | Inserções: {novos}", flush=True)
                    
                    if modo_nav == "RAPIDA" and paginas_sem_novos >= 2:
                        print(f"[LFP] Saturação atingida ({sigla}): Nenhuma amostra nova.", flush=True)
                        break
                    elif modo_nav == "PROFUNDA" and paginas_sem_novos >= 10:
                        print(f"[LFP] Saturação atingida na Varredura Profunda ({sigla}).", flush=True)
                        break
                    
                    links_da_pagina_anterior = links_da_pagina_atual
                    time.sleep(random.uniform(2, 5))
                    pag += 1
                    
                except Exception as e:
                    erros_consecutivos += 1
                    print(f"[LFP] ERRO pag {pag}: {str(e)[:50]}", flush=True)
                    time.sleep(5)
                    
        browser.close()
    conn.close()
    
    print(f"[LFP] Operação Finalizada. Total de novas inserções: {total_geral_novos}.", flush=True)
    if evento_fim: evento_fim.set()

if __name__ == "__main__":
    tarefas = obter_tarefas_ativas()
    executar_lfp(tarefas)