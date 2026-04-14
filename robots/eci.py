# =====================================================================
# ARQUIVO: robots/eci.py
# MÓDULO: ECI (Extrator e Classificador Inteligente)
# =====================================================================

import os
import time
import random
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

import instructor
from openai import OpenAI
from pydantic import BaseModel, Field

from database.connection import obter_conexao
from robots.aci import buscar_mun_id

try:
    from playwright_stealth import stealth_sync
    aplicar_stealth = stealth_sync
except ImportError:
    aplicar_stealth = None

load_dotenv()

class AnuncioFazendaIA(BaseModel):
    titulo: str | None = Field(description="Título completo do anúncio.")
    valor_total: float | None = Field(description="Valor em reais (float).")
    area_bruta: float | None = Field(
        description="""Área total (float). Extraia o número puro. 
        Ignore erros de português. Considere: Hectares, Alqueires, Tarefas e Litros."""
    )
    unm_id: int | None = Field(
        description="ID da unidade: 1=Hectare, 2=Alq. Paulista, 3=Alq. Mineiro, 4=Alq. Baiano, 5=m2, 6=Tarefa, 7=Litro. Padrão: 1."
    )
    municipio: str | None = Field(description="Nome da cidade (corrigido).")
    estado_sigla: str | None = Field(description="Sigla UF (2 letras).")
    tipologias_ids: list[int] = Field(default_factory=list, description="IDs de Tipologia.")

def obter_tarefa_da_fila(conn):
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE public.smt_fila_processamento
        SET fil_status = 'PROCESSANDO', fil_data_atualizacao = CURRENT_TIMESTAMP
        WHERE fil_id = (SELECT fil_id FROM public.smt_fila_processamento WHERE fil_status = 'PENDENTE' LIMIT 1 FOR UPDATE SKIP LOCKED)
        RETURNING fil_id, fil_fonte, fil_url;
    """)
    t = cursor.fetchone()
    conn.commit()
    cursor.close()
    return t

def finalizar_tarefa(conn, fil_id, status, erro=None):
    cursor = conn.cursor()
    cursor.execute("UPDATE public.smt_fila_processamento SET fil_status = %s, fil_data_atualizacao = CURRENT_TIMESTAMP WHERE fil_id = %s", (status, fil_id))
    if erro:
        cursor.execute("INSERT INTO public.smt_anuncio_erro (err_fil_id, err_motivo) VALUES (%s, %s)", (fil_id, erro))
    conn.commit()
    cursor.close()

def salvar_anuncio_final(conn, fil_id, d):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO public.smt_anuncio (anc_origem, anc_link, anc_titulo, anc_desc, anc_valor_total, anc_hectare, anc_municipio_raw, anc_uf_raw, anc_mun_id, anc_fil_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (anc_link) DO NOTHING;
    """, (d['origem'], d['link'], d['titulo'], d['desc'], d['preco_real'], d['area_ha'], d['mun'], d['uf'], d['mun_id'], fil_id))
    
    cursor.execute("SELECT anc_id FROM public.smt_anuncio WHERE anc_link = %s", (d['link'],))
    res = cursor.fetchone()
    anc_id = res[0] if res else None
    conn.commit()
    cursor.close()
    return anc_id

def salvar_tipologias(conn, anc_id, tip_ids):
    if not tip_ids or not anc_id: return
    cursor = conn.cursor()
    cursor.execute("DELETE FROM public.smt_anuncio_tipologia WHERE atp_anc_id = %s", (anc_id,))
    for t_id in tip_ids:
        cursor.execute("INSERT INTO public.smt_anuncio_tipologia (atp_anc_id, atp_tip_id) VALUES (%s, %s)", (anc_id, t_id))
    conn.commit()
    cursor.close()

def obter_unidades_medida(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT unm_id, unm_nome, unm_fator_hectare FROM public.smt_unidade_medida")
    return cursor.fetchall()

def obter_tipologias(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT tip_id, tip_nome FROM public.smt_tipologia ORDER BY tip_id")
    return cursor.fetchall()

JS_EXTRAIR_TEXTO = """() => {
    function getX(path) {
        try {
            let n = document.evaluate(path, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
            return n ? n.innerText.trim() : null;
        } catch(e) { return null; }
    }
    let c = getX('//*[@id="adview"]/div[3]/div/div[1]/div/div/nav/ol'); 
    let v1 = getX('//*[@id="price-box-container"]/div[1]/div[1]/div/span/span'); 
    let t = getX('//*[@id="description-title"]/div/div[1]/div/span'); 
    let d = getX('//*[@id="description-title"]/div/div[2]/div/span/span'); 
    
    let prompt_ia = "[ESTRUTURADO]\\nTítulo: "+t+"\\nPreço: "+v1+"\\nCidade: "+c+"\\nDescrição: "+d;
    let bodyText = document.body.innerText; 
    prompt_ia += "\\n[FALLBACK]\\n" + bodyText.substring(0, 3000);
    return { "prompt_ia": prompt_ia, "desc_pura": d ? d : bodyText.substring(0, 4000) };
}"""

def auditar_xpaths_olx():
    print("[SYSTEM] Iniciando Auditoria de XPaths...", flush=True)
    conn = obter_conexao()
    cursor = conn.cursor()
    cursor.execute("SELECT fil_url FROM public.smt_fila_processamento LIMIT 1")
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        print("[SYSTEM] Fila vazia. Auditoria abortada.", flush=True)
        return
        
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        page = browser.new_page()
        page.goto(row[0], timeout=60000)
        time.sleep(4)
        print("[SYSTEM] Auditoria Concluída.", flush=True)
        browser.close()

def executar_eci_worker(worker_id=1, evento_lfp_fim=None, limite_isolado=None):
    conn = obter_conexao()
    
    cliente_ia = instructor.from_openai(
        OpenAI(base_url="http://localhost:11434/v1", api_key="ollama_local"),
        mode=instructor.Mode.JSON,
    )

    unidades_db = obter_unidades_medida(conn)
    dict_fatores = {u[0]: float(u[2]) if u[2] else 1.0 for u in unidades_db}
    dict_fatores[6] = 0.43; dict_fatores[7] = 0.0484 
    
    prompt_unidades = "".join([f"ID {u[0]} - {u[1]}\n" for u in unidades_db])
    system_prompt = f"Você é um Perito Agrário. Extraia dados. Unidades:\n{prompt_unidades}"

    print(f"Worker {worker_id}: Inicializado.", flush=True)
    
    modo_headless = os.getenv("SIMET_HEADLESS", "0") == "1"
    processados = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=modo_headless, args=["--no-sandbox"])
        context = browser.new_context(viewport={'width': 1366, 'height': 768})
        
        while True:
            if limite_isolado and processados >= limite_isolado: break
            
            tarefa = obter_tarefa_da_fila(conn)
            if not tarefa:
                if evento_lfp_fim and evento_lfp_fim.is_set(): break
                time.sleep(5); continue
                    
            fil_id, fonte, url = tarefa
            try:
                page = context.new_page()
                if aplicar_stealth: aplicar_stealth(page)
                
                page.goto(url, timeout=60000, wait_until="domcontentloaded")
                time.sleep(random.uniform(3, 5))
                dados_pagina = page.evaluate(JS_EXTRAIR_TEXTO)
                
                res_ia = cliente_ia.chat.completions.create(
                    model="llama3.1",
                    messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": dados_pagina["prompt_ia"]}],
                    response_model=AnuncioFazendaIA, temperature=0.0 
                )
                
                area_final_ha = round(res_ia.area_bruta * dict_fatores.get(res_ia.unm_id, 1.0), 4) if res_ia.area_bruta else None
                mun_id_db = buscar_mun_id(conn, res_ia.municipio, res_ia.estado_sigla)
                
                if res_ia.valor_total and area_final_ha and mun_id_db:
                    anc_id = salvar_anuncio_final(conn, fil_id, {
                        'origem': fonte, 'link': url, 'titulo': res_ia.titulo, 'desc': dados_pagina["desc_pura"][:4000], 
                        'preco_real': res_ia.valor_total, 'area_ha': area_final_ha, 'mun': res_ia.municipio, 
                        'uf': res_ia.estado_sigla, 'mun_id': mun_id_db
                    })
                    if res_ia.tipologias_ids: salvar_tipologias(conn, anc_id, res_ia.tipologias_ids)
                    finalizar_tarefa(conn, fil_id, 'CONCLUIDO')
                    print(f"Worker {worker_id}: [SUCESSO] ID {fil_id} | {area_final_ha}ha - R${res_ia.valor_total}", flush=True)
                else:
                    finalizar_tarefa(conn, fil_id, 'ERRO', "IA Falhou")
                    print(f"Worker {worker_id}: [REJEITADO] ID {fil_id}", flush=True)
                    
            except Exception as e:
                finalizar_tarefa(conn, fil_id, 'ERRO', str(e)[:50])
                print(f"Worker {worker_id}: [ERRO] ID {fil_id} - {str(e)[:40]}", flush=True)
            finally:
                page.close()
                processados += 1
                
        browser.close()
    conn.close()

def executar_eci_separado(limite=None):
    executar_eci_worker(worker_id=1, evento_lfp_fim=None, limite_isolado=limite)