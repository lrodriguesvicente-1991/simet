# =====================================================================
# ARQUIVO: database/engine.py
# DESCRIÇÃO: Motor central de processamento, queries e inteligência.
# =====================================================================
import re
import json
import unicodedata
from database.connection import obter_conexao

_MUN_CACHE = {}
_CLASS_CACHE = None
_MUN_LISTA_CACHE = {}

# --- UTILITÁRIOS DE TEXTO E GEOGRAFIA (Herdado do ACI Original) ---
def normalizar_texto(texto):
    if not texto: return ""
    texto = str(texto).strip().lower()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def buscar_mun_id(conn, mun_raw, uf_raw):
    if not mun_raw or not uf_raw: return None
    mun_norm = normalizar_texto(mun_raw)
    uf_norm = str(uf_raw).strip().upper()

    # Usa o Cache em Memória para poupar o banco (Performance)
    if uf_norm not in _MUN_CACHE:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT m.mun_id, m.mun_nome 
            FROM public.smt_municipio m
            JOIN public.smt_unidade_federativa u ON m.mun_unf_id = u.unf_id
            WHERE u.unf_sigla = %s
        """, (uf_norm,))
        # Salva a lista normalizada no cache
        _MUN_CACHE[uf_norm] = [(r[0], normalizar_texto(r[1])) for r in cursor.fetchall()]
        cursor.close()

    municipios = _MUN_CACHE[uf_norm]

    # Tenta match exato primeiro
    for mun_id, mun_nome in municipios:
        if mun_nome == mun_norm: return mun_id
    # Fallback
    for mun_id, mun_nome in municipios:
        if mun_nome in mun_norm: return mun_id
    return None

def listar_municipios_uf(conn, uf):
    """Retorna lista de nomes de municipios da UF (ordem alfabetica, com acentos).
    Usada para injetar no prompt e limitar o universo de respostas da IA."""
    if not uf: return []
    uf_norm = str(uf).strip().upper()
    if uf_norm in _MUN_LISTA_CACHE:
        return _MUN_LISTA_CACHE[uf_norm]
    cursor = conn.cursor()
    cursor.execute("""
        SELECT m.mun_nome FROM public.smt_municipio m
        JOIN public.smt_unidade_federativa u ON m.mun_unf_id = u.unf_id
        WHERE u.unf_sigla = %s
        ORDER BY m.mun_nome
    """, (uf_norm,))
    nomes = [r[0] for r in cursor.fetchall()]
    cursor.close()
    _MUN_LISTA_CACHE[uf_norm] = nomes
    return nomes

_PADROES_AREA = [
    (re.compile(r'([\d\.,]+)\s*(?:hectare|hectares|ha|hec|hactare)\b', re.IGNORECASE), 1.0),
    (re.compile(r'([\d\.,]+)\s*(?:alqueire paulista|alq paulista|alq sp)\b', re.IGNORECASE), 2.42),
    (re.compile(r'([\d\.,]+)\s*(?:alqueire mineiro|alq mineiro|alq goiano|alq mg|alq go)\b', re.IGNORECASE), 4.84),
    (re.compile(r'([\d\.,]+)\s*(?:alqueire baiano|alq baiano|alq nordeste)\b', re.IGNORECASE), 9.68),
    (re.compile(r'([\d\.,]+)\s*(?:alqueire|alqueires|alq)\b', re.IGNORECASE), 2.42),
    (re.compile(r'([\d\.,]+)\s*(?:tarefa|tarefas|trf)\b', re.IGNORECASE), 0.43),
    (re.compile(r'([\d\.,]+)\s*(?:litro|litros|lts)\b', re.IGNORECASE), 0.0484),
    (re.compile(r'([\d\.,]+)\s*(?:m²|m2|metros\s*quadrados|m\s*²)', re.IGNORECASE), 0.0001),
]

def extrair_area_regex_fallback(texto_descricao):
    """Fallback seguro usando a sua lógica Regex original caso a IA falhe."""
    if not texto_descricao: return None
    texto_busca = str(texto_descricao)
    for padrao, multiplicador in _PADROES_AREA:
        match = padrao.search(texto_busca)
        if match:
            v_str = match.group(1).replace('.', '').replace(',', '.')
            try:
                return round(float(v_str) * multiplicador, 2)
            except ValueError:
                continue
    return None


_PADROES_ISCA = [
    r'\bentrada\s+de\s+r\$',
    r'\bentrada\s*[:\-]?\s*r\$',
    r'\bsinal\s+de\s+r\$',
    r'\ba\s+partir\s+de\s+r\$',
    r'\bparcela(s)?\s+de\s+r\$',
    r'\b\d+\s*x\s+r\$',
    r'\bfinancia(mento|do|\s+em)\b',
    r'\b\d+\s*(meses|vezes|parcelas)\b',
]
_RE_ISCA = re.compile('|'.join(_PADROES_ISCA), re.IGNORECASE)


def detectar_isca(texto):
    """True se o texto indica valor parcelado/entrada (nao e valor total)."""
    if not texto: return False
    return bool(_RE_ISCA.search(texto))


_RE_VALOR = re.compile(r'r\$\s*([\d\.]+(?:,\d{1,2})?)', re.IGNORECASE)


def extrair_valor_total_regex(texto):
    """Tenta extrair um valor R$ do texto que NAO esteja colado a entrada/parcela.
    Heuristica: pega o MAIOR R$ do texto (o valor total normalmente e o maior).
    Retorna float ou None."""
    if not texto: return None
    maior = None
    for m in _RE_VALOR.finditer(str(texto)):
        bruto = m.group(1).replace('.', '').replace(',', '.')
        try:
            v = float(bruto)
            if v >= 10000 and (maior is None or v > maior):
                maior = v
        except ValueError:
            continue
    return maior

# --- OPERAÇÕES LFP (Fila) ---
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

def salvar_na_fila(conn, fonte, url, dados_json=None):
    cursor = conn.cursor()
    dados_str = json.dumps(dados_json, ensure_ascii=False) if dados_json else None
    cursor.execute("""
        INSERT INTO public.smt_fila_processamento (fil_fonte, fil_url, fil_conteudo_jsonb, fil_status)
        VALUES (%s, %s, %s, 'PENDENTE') ON CONFLICT (fil_url) DO NOTHING;
    """, (fonte, url, dados_str))
    inserido = cursor.rowcount > 0
    conn.commit()
    cursor.close()
    return inserido

# --- OPERAÇÕES ECI (Extração) ---
def obter_tarefa_da_fila(conn):
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE public.smt_fila_processamento
        SET fil_status = 'PROCESSANDO', fil_data_atualizacao = CURRENT_TIMESTAMP
        WHERE fil_id = (SELECT fil_id FROM public.smt_fila_processamento WHERE fil_status = 'PENDENTE' ORDER BY fil_id DESC LIMIT 1 FOR UPDATE SKIP LOCKED)
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
        INSERT INTO public.smt_anuncio (
            anc_origem, anc_link, anc_titulo, anc_desc, anc_valor_total,
            anc_hectare, anc_municipio_raw, anc_uf_raw, anc_mun_id, anc_fil_id,
            anc_data_publicacao_texto, anc_data_publicacao
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (anc_link) DO UPDATE SET
            anc_valor_total = EXCLUDED.anc_valor_total,
            anc_desc = EXCLUDED.anc_desc,
            anc_hectare = EXCLUDED.anc_hectare,
            anc_data_publicacao_texto = EXCLUDED.anc_data_publicacao_texto,
            anc_data_publicacao = COALESCE(EXCLUDED.anc_data_publicacao, smt_anuncio.anc_data_publicacao),
            anc_data_processamento = CURRENT_TIMESTAMP;
    """, (
        d['origem'], d['link'], d['titulo'], d['desc'], d['preco_real'],
        d['area_ha'], d['mun'], d['uf'], d['mun_id'], fil_id,
        d['data_publicacao_texto'], d.get('data_publicacao')
    ))
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
    resultado = cursor.fetchall()
    cursor.close()
    return resultado

# --- CLASSIFICAÇÃO DE TIPOLOGIAS (Dicionário + Pesos) ---
def obter_termos_classificacao(conn):
    global _CLASS_CACHE
    if _CLASS_CACHE is not None:
        return _CLASS_CACHE
    cursor = conn.cursor()
    cursor.execute("""
        SELECT clp_termo, clp_tip_id, clp_peso
        FROM public.smt_class_pesquisa
        WHERE clp_ativo = true
    """)
    _CLASS_CACHE = [(normalizar_texto(r[0]), r[1], r[2]) for r in cursor.fetchall()]
    cursor.close()
    return _CLASS_CACHE

def classificar_tipologias(conn, texto, threshold=5):
    if not texto: return []
    termos = obter_termos_classificacao(conn)
    texto_norm = normalizar_texto(texto)
    scores = {}
    for termo_norm, tip_id, peso in termos:
        if termo_norm and termo_norm in texto_norm:
            scores[tip_id] = scores.get(tip_id, 0) + peso
    return [tip_id for tip_id, score in scores.items() if score >= threshold]

# --- OPERAÇÕES ACI NOVO (Auditoria) ---
def obter_erros_para_auditoria(conn, limite=50):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT e.err_id, f.fil_id, f.fil_url, e.err_motivo
        FROM public.smt_anuncio_erro e
        JOIN public.smt_fila_processamento f ON e.err_fil_id = f.fil_id
        WHERE f.fil_status = 'ERRO'
          AND e.err_motivo NOT LIKE 'LINK_INATIVO:%%'
          AND e.err_motivo NOT LIKE 'DESCARTADO:%%'
        LIMIT %s
    """, (limite,))
    res = cursor.fetchall()
    cursor.close()
    return res

def aprovar_reciclagem(conn, err_id, fil_id):
    cursor = conn.cursor()
    cursor.execute("UPDATE public.smt_fila_processamento SET fil_status = 'PENDENTE' WHERE fil_id = %s", (fil_id,))
    cursor.execute("DELETE FROM public.smt_anuncio_erro WHERE err_id = %s", (err_id,))
    conn.commit()
    cursor.close()

def descartar_link_morto(conn, err_id, fil_id, motivo):
    cursor = conn.cursor()
    cursor.execute("UPDATE public.smt_anuncio_erro SET err_motivo = %s WHERE err_id = %s", (f"LINK_INATIVO: {motivo}", err_id))
    conn.commit()
    cursor.close()