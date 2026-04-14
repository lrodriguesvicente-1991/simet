# =====================================================================
# ARQUIVO: api.py
# DESCRIÇÃO: API RESTful (FastAPI) para o Monolito Modular.
# Fornece os dados do PostGIS em JSON para o React e controla os Robôs.
# =====================================================================

import os
import sys
import psutil
import subprocess
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
from dotenv import load_dotenv

from database.connection import obter_conexao

load_dotenv()

app = FastAPI(title="SIMET API", version="2.0")

# =====================================================================
# CONFIGURAÇÃO DE CORS (Permite que o React converse com a API local)
# =====================================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Na produção, colocaremos o domínio exato aqui
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =====================================================================
# SISTEMA DE AUTENTICAÇÃO (LOGIN)
# =====================================================================
class LoginRequest(BaseModel):
    usuario: str
    senha: str

@app.post("/api/login")
def fazer_login(req: LoginRequest):
    """Valida as credenciais e devolve um Token de Acesso"""
    # Credenciais Master (No futuro, podemos ler do banco de dados)
    USUARIO_OFICIAL = "incra"
    SENHA_OFICIAL = "simet2026"
    
    if req.usuario == USUARIO_OFICIAL and req.senha == SENHA_OFICIAL:
        # Gera um crachá de acesso (Token)
        return {"sucesso": True, "token": "simet-master-token-validado"}
    else:
        raise HTTPException(status_code=401, detail="Usuário ou senha incorretos")
    
# =====================================================================
# CONTROLE DE ESTADO DOS ROBÔS (Memória da API)
# =====================================================================
estado_robo = {
    "rodando": False,
    "processo_pid": None,
    "logs": []
}

class OperacaoRequest(BaseModel):
    task: str
    workers: int = 1

# =====================================================================
# ROTAS DE DADOS (DASHBOARD)
# =====================================================================
@app.get("/api/dados")
def get_dashboard_data():
    """Busca os dados mastigados da View e converte para JSON para o React"""
    try:
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
        
        # Correção Anti-Bug (JSON compliant):
        # Converte valores nulos e erros matemáticos (NaN) do Pandas para None do Python
        df = df.replace({pd.NA: None, float('nan'): None})
        
        return {"sucesso": True, "dados": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/sincronizar")
def sincronizar_view():
    """Atualiza a Materialized View com novos cálculos matemáticos"""
    try:
        conn = obter_conexao()
        conn.autocommit = True 
        cur = conn.cursor()
        cur.execute("REFRESH MATERIALIZED VIEW public.mv_estatisticas_simet;")
        cur.close()
        conn.close()
        return {"sucesso": True, "mensagem": "Base de dados sincronizada com sucesso!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =====================================================================
# ROTAS DE AUTOMAÇÃO (ROBÔS LFP E ECI)
# =====================================================================
@app.post("/api/robo/iniciar")
def iniciar_robo(req: OperacaoRequest):
    """Inicia o subprocesso do main.py (Exatamente como o Streamlit fazia)"""
    global estado_robo
    
    if estado_robo["rodando"]:
        raise HTTPException(status_code=400, detail="Uma operação já está em andamento.")
        
    comando = [sys.executable, "-u", "main.py", "--task", req.task, "--workers", str(req.workers)]
    env_utf8 = os.environ.copy()
    env_utf8["PYTHONIOENCODING"] = "utf-8"
    
    try:
        # Abre o processo em background
        processo = subprocess.Popen(
            comando, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
            text=True, bufsize=1, env=env_utf8, encoding='utf-8'
        )
        
        estado_robo["rodando"] = True
        estado_robo["processo_pid"] = processo.pid
        estado_robo["logs"] = [f"[SYSTEM] Iniciando operação: {req.task.upper()}"]
        
        return {"sucesso": True, "mensagem": "Robôs iniciados!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao iniciar robô: {e}")

@app.post("/api/robo/parar")
def parar_robo():
    """Mata o processo dos robôs"""
    global estado_robo
    
    if not estado_robo["rodando"] or not estado_robo["processo_pid"]:
        return {"sucesso": False, "mensagem": "Nenhum robô rodando no momento."}
        
    try:
        parent = psutil.Process(estado_robo["processo_pid"])
        for child in parent.children(recursive=True): 
            child.kill()
        parent.kill()
    except psutil.NoSuchProcess:
        pass
        
    estado_robo["rodando"] = False
    estado_robo["processo_pid"] = None
    estado_robo["logs"].append("[SYSTEM] Operação abortada pelo usuário.")
    
    return {"sucesso": True, "mensagem": "Robôs parados com sucesso!"}

@app.get("/api/robo/status")
def status_robo():
    """Retorna se o robô está rodando e os logs atuais. Ideal para o Front-end monitorar"""
    global estado_robo
    return {
        "rodando": estado_robo["rodando"],
        "logs_recentes": estado_robo["logs"][-20:] # Retorna as últimas 20 linhas
    }