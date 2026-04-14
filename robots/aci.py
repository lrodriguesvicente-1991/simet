# =====================================================================
# ARQUIVO: robots/aci.py
# MÓDULO: ACI (Analisador de Conteúdo Institucional) / RegEx Fallback
# =====================================================================

import re
import unicodedata
from database.connection import obter_conexao

def normalizar_texto(texto):
    if not texto: return ""
    texto = str(texto).strip().lower()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def buscar_mun_id(conn, mun_raw, uf_raw):
    if not mun_raw or not uf_raw: return None
    
    mun_norm = normalizar_texto(mun_raw)
    uf_norm = str(uf_raw).strip().upper()

    cursor = conn.cursor()
    cursor.execute("""
        SELECT m.mun_id, m.mun_nome 
        FROM public.smt_municipio m
        JOIN public.smt_unidade_federativa u ON m.mun_unf_id = u.unf_id
        WHERE u.unf_sigla = %s
    """, (uf_norm,))
    municipios = cursor.fetchall()
    cursor.close()

    for mun_id, mun_nome in municipios:
        if normalizar_texto(mun_nome) == mun_norm: return mun_id
    for mun_id, mun_nome in municipios:
        if normalizar_texto(mun_nome) in mun_norm: return mun_id
            
    return None

def extrair_valor_real(preco_raw, descricao):
    preco_limpo = None
    if preco_raw:
        p_str = str(preco_raw).lower().replace('r$', '').replace('.', '').replace(',', '.').strip()
        match = re.search(r'(\d+\.?\d*)', p_str)
        if match: preco_limpo = float(match.group(1))
            
    if (not preco_limpo or preco_limpo < 1000) and descricao:
        padroes = [r'(?:r\$|valor|pedindo|quero)[\s:]*([\d]{1,3}(?:\.[\d]{3})*(?:,\d{1,2})?)']
        for padrao in padroes:
            matches = re.findall(padrao, descricao.lower())
            if matches:
                valores = [float(m.replace('.', '').replace(',', '.')) for m in matches]
                valores_validos = [v for v in valores if v > 1000]
                if valores_validos: return max(valores_validos)
    return preco_limpo

def extrair_area_hectares(area_raw, descricao):
    area_ha = None
    texto_busca = f"{area_raw} {descricao}".lower()
    
    padroes = [
        (r'([\d\.,]+)\s*(?:hectare|hectares|ha|hec|hactare)\b', 1.0),
        (r'([\d\.,]+)\s*(?:alqueire paulista|alq paulista|alq sp)\b', 2.42),
        (r'([\d\.,]+)\s*(?:alqueire mineiro|alq mineiro|alq goiano|alq mg|alq go)\b', 4.84),
        (r'([\d\.,]+)\s*(?:alqueire baiano|alq baiano|alq nordeste)\b', 9.68),
        (r'([\d\.,]+)\s*(?:alqueire|alqueires|alq)\b', 2.42), 
        (r'([\d\.,]+)\s*(?:tarefa|tarefas|trf)\b', 0.43),      
        (r'([\d\.,]+)\s*(?:litro|litros|lts)\b', 0.0484),     
        (r'([\d\.,]+)\s*(?:m2|m2|metros quadrados)\b', 0.0001)
    ]
    
    for padrao, multiplicador in padroes:
        match = re.search(padrao, texto_busca)
        if match:
            v_str = match.group(1).replace('.', '').replace(',', '.')
            try: return round(float(v_str) * multiplicador, 2)
            except: continue
    return area_ha

def limpar_anuncio(dados_brutos, conn):
    dados_limpos = dados_brutos.copy()
    dados_limpos['preco_real'] = extrair_valor_real(dados_brutos.get('preco'), dados_brutos.get('desc'))
    dados_limpos['area_ha'] = extrair_area_hectares(dados_brutos.get('area'), dados_brutos.get('desc'))
    dados_limpos['mun_id'] = buscar_mun_id(conn, dados_brutos.get('mun'), dados_brutos.get('uf'))
    return dados_limpos

def executar_aci_separado():
    print("[ACI] Analisador de Conteudo Institucional executado.", flush=True)