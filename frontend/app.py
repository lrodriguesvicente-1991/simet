# =====================================================================
# ARQUIVO: frontend/app.py
# DESCRIÇÃO: Dashboard Analítico (Adaptativo Light/Dark)
# =====================================================================

import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import os
import subprocess
import sys
import base64
import psutil
import re
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.connection import obter_conexao

# =====================================================================
# CONFIGURAÇÃO INICIAL E CSS
# =====================================================================
st.set_page_config(page_title="SIMET - INCRA", layout="wide", page_icon="frontend/assets/icon_incra.png")

try:
    with open("frontend/styles/main.css", "r") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
except Exception:
    pass

load_dotenv()

# =====================================================================
# DADOS
# =====================================================================
@st.cache_data(ttl=300)
def load_data():
    conn = obter_conexao()
    query = """
    SELECT 
        municipio, estado, regiao, categoria_tamanho, total_anuncios_reais, 
        mediana_geral, media_geral, desvio_padrao, coef_dispersao_pct,
        ST_Y(ST_Centroid(geom_municipio::geometry)) as lat,
        ST_X(ST_Centroid(geom_municipio::geometry)) as lon
    FROM public.vw_media_mercado_terras 
    WHERE geom_municipio IS NOT NULL;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def atualizar_view_materializada():
    try:
        conn = obter_conexao()
        conn.autocommit = True 
        cur = conn.cursor()
        cur.execute("REFRESH MATERIALIZED VIEW public.mv_estatisticas_simet;")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        return str(e)

if 'robot_running' not in st.session_state: st.session_state.robot_running = False
if 'log_geral' not in st.session_state: st.session_state.log_geral = []
if 'log_eci_dict' not in st.session_state: st.session_state.log_eci_dict = {}

running = st.session_state.robot_running
df_base = load_data()

def get_image_base64(path):
    try:
        with open(path, "rb") as img_file: return base64.b64encode(img_file.read()).decode()
    except: return ""

b64_incra = get_image_base64("frontend/assets/icon_incra.png")

# =====================================================================
# SIDEBAR (MENU LATERAL)
# =====================================================================
with st.sidebar:
    if b64_incra:
        st.markdown(f"""
        <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 30px;">
            <img src='data:image/png;base64,{b64_incra}' style='height: 40px;'>
            <h2 style='margin:0; font-size: 1.5rem; color: #005826;'>SIMET</h2>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<p style='font-size: 0.8rem; font-weight: bold; color: gray; text-transform: uppercase;'>Navegação</p>", unsafe_allow_html=True)
    
    # Texto limpo, sem emojis ou ícones estranhos
    menu_selecionado = st.radio(
        "",
        ["Dashboard", "Automação e Robôs"],
        label_visibility="collapsed"
    )

# =====================================================================
# TELA 1: DASHBOARD
# =====================================================================
if menu_selecionado == "Dashboard":
    
    h_col1, h_col2 = st.columns([8, 2])
    with h_col1:
        st.markdown("<h2 style='margin-bottom: 0px; font-weight: 800;'>SIMET - Inteligência do Mercado de Terras</h2>", unsafe_allow_html=True)
        st.markdown("<p style='color: gray; font-size: 0.95rem;'>Plataforma Analítica e Econométrica de Valores Fundiários</p>", unsafe_allow_html=True)
    with h_col2:
        st.markdown("<div style='margin-top: 15px;'></div>", unsafe_allow_html=True)
        if st.button("🔄 Sincronizar Análises", use_container_width=True):
            with st.spinner("Atualizando estatísticas..."):
                res = atualizar_view_materializada()
                if res is True:
                    st.cache_data.clear(); st.rerun()
                else: st.error(res)

    st.write("")

    # Filtros agora respeitam o tamanho nativo do Streamlit (menores e elegantes)
    with st.container():
        f_col1, f_col2, f_col3, f_col4 = st.columns(4)
        with f_col1: sel_regiao = st.selectbox("Região", ["Todas"] + sorted(df_base['regiao'].dropna().unique().tolist()))
        df_filtrado = df_base if sel_regiao == "Todas" else df_base[df_base['regiao'] == sel_regiao]
        
        with f_col2: sel_estado = st.selectbox("Estado", ["Todos"] + sorted(df_filtrado['estado'].dropna().unique().tolist()))
        df_filtrado = df_filtrado if sel_estado == "Todos" else df_filtrado[df_filtrado['estado'] == sel_estado]
        
        with f_col3: sel_cat = st.selectbox("Tamanho da Terra", ["Todos"] + sorted(df_filtrado['categoria_tamanho'].dropna().unique().tolist()))
        df_filtrado = df_filtrado if sel_cat == "Todos" else df_filtrado[df_filtrado['categoria_tamanho'] == sel_cat]
        
        with f_col4: sel_municipio = st.selectbox("Município", ["Todos"] + sorted(df_filtrado['municipio'].dropna().unique().tolist()))
        if sel_municipio != "Todos": df_filtrado = df_filtrado[df_filtrado['municipio'] == sel_municipio]

    mediana_brasil = df_filtrado['mediana_geral'].median()
    media_brasil = df_filtrado['media_geral'].mean()
    total_anuncios = df_filtrado['total_anuncios_reais'].sum()
    dispersao_media = df_filtrado['coef_dispersao_pct'].mean()
    
    val_mediana = f"R$ {mediana_brasil:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notnull(mediana_brasil) else "R$ 0,00"
    val_media = f"R$ {media_brasil:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notnull(media_brasil) else "R$ 0,00"
    val_amostras = f"{total_anuncios:,}".replace(",", ".")
    val_disp = f"{dispersao_media:.1f}%" if pd.notnull(dispersao_media) else "0%"

    # KPIs unificados para se adaptarem à cor atual do tema
    html_kpis = f"""
    <div class="kpi-container">
        <div class="kpi-card">
            <div class="kpi-title">Mediana (R$/ha)</div>
            <div class="kpi-value">{val_mediana}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-title">Preço Médio (R$/ha)</div>
            <div class="kpi-value">{val_media}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-title">Volume Amostral (n)</div>
            <div class="kpi-value">{val_amostras}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-title">Volatilidade/Dispersão</div>
            <div class="kpi-value">{val_disp}</div>
        </div>
    </div>
    """
    st.markdown(html_kpis, unsafe_allow_html=True)

    map_col, table_col = st.columns([4, 6]) 
    
    with map_col:
        st.markdown("<div class='section-title'>Mapa Geográfico</div>", unsafe_allow_html=True)
        if sel_municipio != "Todos": zoom, cor_destaque = 11, "#005826"
        elif sel_estado != "Todos": zoom, cor_destaque = 6, "#688A3A"
        else: zoom, cor_destaque = 4, "#5ca367"
        
        center_lat = df_filtrado['lat'].mean() if not df_filtrado.empty else -14.235
        center_lon = df_filtrado['lon'].mean() if not df_filtrado.empty else -51.925

        m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom, tiles="cartodbpositron")
        
        for _, row in df_filtrado.head(1000).iterrows():
            folium.CircleMarker(
                location=[row['lat'], row['lon']], radius=7, color=cor_destaque, fill=True, fill_color=cor_destaque, fill_opacity=0.7,
                tooltip=f"<b>{row['municipio']} ({row['estado']})</b><br>Mediana: R$ {row['mediana_geral']:,.2f}"
            ).add_to(m)
            
        st_folium(m, use_container_width=True, height=400, returned_objects=[])

    with table_col:
        st.markdown("<div class='section-title'>Dados por Categoria</div>", unsafe_allow_html=True)
        df_view = df_filtrado[['municipio', 'estado', 'categoria_tamanho', 'total_anuncios_reais', 'mediana_geral', 'coef_dispersao_pct']].copy()
        
        def format_brl(val):
            return f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notnull(val) else "-"
            
        df_view['mediana_geral'] = df_view['mediana_geral'].apply(format_brl)
        df_view['coef_dispersao_pct'] = df_view['coef_dispersao_pct'].apply(lambda x: f"{x:.1f}%" if pd.notnull(x) else "-")

        st.dataframe(
            df_view, use_container_width=True, height=400, hide_index=True,
            column_config={
                "municipio": "Município", "estado": "UF", "categoria_tamanho": "Categoria",
                "total_anuncios_reais": "Amostras (n)", "mediana_geral": "Mediana", "coef_dispersao_pct": "Dispersão"
            }
        )

# =====================================================================
# TELA 2: AUTOMAÇÃO E EXTRAÇÃO
# =====================================================================
elif menu_selecionado == "Automação e Robôs":
    st.markdown("<h3 style='margin-top:0;'>Centro de Comando da Operação</h3>", unsafe_allow_html=True)
    st.markdown("<p style='color: gray; margin-bottom: 25px;'>Controle manual dos motores de raspagem e inteligência artificial.</p>", unsafe_allow_html=True)
    
    with st.container(border=True):
        cfg1, cfg2 = st.columns(2)
        with cfg1:
            opcoes_operacao = {
                "Completo (LFP + ECI)": "full", 
                "Apenas Mapear Links (LFP)": "lfp", 
                "Apenas Inteligência Artificial (ECI)": "eci", 
                "Fallback Segurança (ACI)": "aci",
                "Auditoria de Rota (Check)": "audit",
                "Teste de Conexão Banco": "test"
            }
            operacao_selecionada = st.selectbox("Tipo de Operação", list(opcoes_operacao.keys()), disabled=running)
            
        mostrar_workers = operacao_selecionada in ["Completo (LFP + ECI)", "Apenas Inteligência Artificial (ECI)"]
        with cfg2:
            if mostrar_workers:
                workers = st.selectbox("Poder de Processamento (Workers IA)", [1, 2, 3, 4, 5], index=2, disabled=running)
            else:
                workers = 1
                st.write("")

        st.write("")
        if not running:
            if st.button("🚀 INICIAR OPERAÇÃO", type="primary"):
                st.session_state.robot_running = True
                st.session_state.log_geral = ["[SYSTEM] Inicializando orquestração de robôs..."]
                st.session_state.log_eci_dict = {i+1: "⏳ Aguardando fila..." for i in range(workers)}
                
                task_arg = opcoes_operacao[operacao_selecionada]
                comando = [sys.executable, "-u", "main.py", "--task", task_arg]
                if mostrar_workers: comando.extend(["--workers", str(workers)])
                
                env_utf8 = os.environ.copy(); env_utf8["PYTHONIOENCODING"] = "utf-8"; env_utf8["SIMET_FRONTEND"] = "1" 
                
                try:
                    st.session_state.processo = subprocess.Popen(
                        comando, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env_utf8, encoding='utf-8'
                    )
                except Exception as e:
                    st.error(f"Erro Crítico: {e}")
                    st.session_state.robot_running = False
                st.rerun()
        else:
            if st.button("🛑 PARAR RÔBO IMEDIATAMENTE", type="secondary"):
                if 'processo' in st.session_state and st.session_state.processo:
                    try:
                        parent = psutil.Process(st.session_state.processo.pid)
                        for child in parent.children(recursive=True): child.kill()
                        parent.kill()
                    except psutil.NoSuchProcess: pass
                st.session_state.robot_running = False
                st.rerun()

    st.write("")
    
    col_log1, col_log2 = st.columns(2)
    with col_log1:
        st.markdown("<div class='section-title'>📡 Logs do Sistema (LFP)</div>", unsafe_allow_html=True)
        box_geral = st.empty()
    with col_log2:
        st.markdown("<div class='section-title'>🧠 Status dos Motores (ECI)</div>", unsafe_allow_html=True)
        box_eci = st.empty()

    def format_log(lista_logs, limite=12):
        linhas = [str(l).strip() for l in lista_logs[-limite:]] if lista_logs else ["> Standby..."]
        while len(linhas) < limite: linhas.append(" ") 
        return "\n".join(linhas[:limite])

    def format_eci_dict(limite_workers=5):
        linhas = [st.session_state.log_eci_dict.get(i, f"Worker {i}: ⏳ Standby") for i in range(1, workers + 1)] if st.session_state.log_eci_dict else ["> Motor neural inativo..."]
        while len(linhas) < limite_workers: linhas.append(" ")
        return "\n".join(linhas[:limite_workers])

    box_geral.code(format_log(st.session_state.log_geral, 12), language="bash")
    box_eci.code(format_eci_dict(5), language="bash")

    if running:
        while True:
            if 'processo' not in st.session_state or st.session_state.processo is None: break
            linha = st.session_state.processo.stdout.readline()
            
            if not linha and st.session_state.processo.poll() is not None:
                st.session_state.robot_running = False
                st.session_state.log_geral.append("[SYSTEM] 🏁 Operação encerrada pelo Orquestrador.")
                box_geral.code(format_log(st.session_state.log_geral, 12), language="bash")
                st.rerun()
                break
                
            if linha:
                linha = linha.strip()
                if not linha: continue
                
                if linha.startswith("Worker"):
                    match = re.search(r'Worker\s*(\d+):', linha, re.IGNORECASE)
                    if match:
                        w_id = int(match.group(1))
                        st.session_state.log_eci_dict[w_id] = linha
                        box_eci.code(format_eci_dict(5), language="bash")
                else:
                    st.session_state.log_geral.append(linha)
                    box_geral.code(format_log(st.session_state.log_geral, 12), language="bash")