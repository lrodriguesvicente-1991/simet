# =====================================================================
# ARQUIVO: api.py
# DESCRIÇÃO: API RESTful (FastAPI) Segura com JWT e Leitura de Pipe
# =====================================================================

import json
import os
import sys
import psutil
import subprocess
import threading
import jwt
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import pandas as pd
from dotenv import load_dotenv

from database.connection import obter_conexao

load_dotenv()

app = FastAPI(title="SIMET API", version="2.0")

# =====================================================================
# CONFIGURAÇÃO DE CORS SEGURA
# =====================================================================
origins = os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:3000").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =====================================================================
# SISTEMA DE AUTENTICAÇÃO JWT (Seguro e via .ENV)
# =====================================================================
SECRET_KEY = os.getenv("JWT_SECRET_KEY")
if not SECRET_KEY:
    raise RuntimeError("JWT_SECRET_KEY não definida no .env — impossível iniciar a API com segurança.")
ALGORITHM = "HS256"
security = HTTPBearer()

class LoginRequest(BaseModel):
    usuario: str
    senha: str

@app.post("/api/login")
def fazer_login(req: LoginRequest):
    user_env = os.getenv("API_USER")
    pass_env = os.getenv("API_PASS")

    if not user_env or not pass_env:
        raise HTTPException(status_code=500, detail="Credenciais de API não configuradas no .env")

    if req.usuario == user_env and req.senha == pass_env:
        payload = {
            "sub": req.usuario,
            "exp": datetime.now(timezone.utc) + timedelta(hours=24)
        }
        token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
        return {"sucesso": True, "token": token}
    
    return {"sucesso": False, "mensagem": "Credenciais inválidas"}

def verificar_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        return payload["sub"]
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido")

# =====================================================================
# GERENCIAMENTO DE ESTADO E PROCESSOS (Com Threading)
# =====================================================================
estado_robo = {
    "rodando": False,
    "processo": None, # Guarda o objeto Popen
    "logs": []
}

def ler_logs_robo(proc):
    """Lê os logs do stdout em tempo real numa Thread separada (Evita block do OS)"""
    for linha in iter(proc.stdout.readline, ''):
        if linha:
            estado_robo["logs"].append(linha.strip())
            # Mantém apenas os últimos 500 logs para não estourar memória
            if len(estado_robo["logs"]) > 500:
                estado_robo["logs"].pop(0)
    proc.stdout.close()

class RoboRequest(BaseModel):
    task: str
    workers: int = 1
    limit: int = 0
    headless: bool = True
    delay_extra: float = 0.0

@app.post("/api/robo/iniciar")
def iniciar_robo(req: RoboRequest, user: str = Depends(verificar_token)):
    if req.task not in ("lfp", "eci", "aci", "full", "test"):
        raise HTTPException(status_code=400, detail=f"Tarefa invalida: {req.task}")

    if estado_robo["rodando"]:
        # Double check se não morreu silênciosamente
        if estado_robo["processo"] and estado_robo["processo"].poll() is None:
            return {"sucesso": False, "mensagem": "Um processo já está em andamento."}

    try:
        env_utf8 = os.environ.copy()
        env_utf8["PYTHONIOENCODING"] = "utf-8"
        env_utf8["SIMET_HEADLESS"] = "1" if req.headless else "0"
        env_utf8["SIMET_DELAY_EXTRA_S"] = str(max(0.0, req.delay_extra))

        comando = [sys.executable, "-u", "main.py", "--task", req.task, "--workers", str(req.workers)]
        if req.limit > 0: comando.extend(["--limit", str(req.limit)])
        
        processo = subprocess.Popen(
            comando, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
            text=True, bufsize=1, env=env_utf8, encoding='utf-8'
        )
        
        estado_robo["rodando"] = True
        estado_robo["processo"] = processo
        estado_robo["logs"] = [f"[SYSTEM] Iniciando operação: {req.task.upper()}..."]
        
        # Dispara a thread para não bloquear o pipe!
        t = threading.Thread(target=ler_logs_robo, args=(processo,), daemon=True)
        t.start()
        
        return {"sucesso": True, "mensagem": "Robôs iniciados!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao iniciar robô: {e}")

@app.post("/api/robo/parar")
def parar_robo(user: str = Depends(verificar_token)):
    if not estado_robo["rodando"] or not estado_robo["processo"]:
        return {"sucesso": False, "mensagem": "Nenhum robô rodando no momento."}

    pid = estado_robo["processo"].pid
    mortos = 0

    # No Windows, taskkill /F /T mata a árvore inteira (inclusive Chromium do Playwright)
    if sys.platform == "win32":
        try:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True, timeout=10, check=False
            )
            mortos = 1
        except Exception as e:
            estado_robo["logs"].append(f"[SYSTEM] taskkill falhou: {e}")

    # Fallback universal via psutil (caso taskkill falhe ou estejamos em Linux/Mac)
    try:
        parent = psutil.Process(pid)
        filhos = parent.children(recursive=True)
        for child in filhos:
            try: child.kill()
            except psutil.NoSuchProcess: pass
        try: parent.kill()
        except psutil.NoSuchProcess: pass
        psutil.wait_procs([parent] + filhos, timeout=3)
        mortos = 1
    except psutil.NoSuchProcess:
        mortos = 1  # Já morreu — sucesso
    except Exception as e:
        estado_robo["logs"].append(f"[SYSTEM] psutil falhou: {e}")

    estado_robo["rodando"] = False
    estado_robo["processo"] = None
    estado_robo["logs"].append("[SYSTEM] Operação abortada pelo usuário.")

    return {"sucesso": bool(mortos), "mensagem": "Robôs parados com sucesso!" if mortos else "Falha ao matar processo."}

@app.get("/api/robo/status")
def status_robo(user: str = Depends(verificar_token)):
    # Checagem de vida: O processo morreu naturalmente (terminou o limite)?
    if estado_robo["rodando"] and estado_robo["processo"]:
        if estado_robo["processo"].poll() is not None: # != None significa que acabou
            estado_robo["rodando"] = False
            estado_robo["processo"] = None
            estado_robo["logs"].append("[SYSTEM] Processo finalizado naturalmente.")

    return {
        "rodando": estado_robo["rodando"],
        "logs_recentes": estado_robo["logs"][-50:] # Manda as últimas 50 pro Front não pesar
    }

# =====================================================================
# ROTAS DE DASHBOARD (Requerem Token)
# =====================================================================
@app.get("/api/dados")
def get_dashboard_data(user: str = Depends(verificar_token)):
    """Busca os dados mastigados da View e converte para JSON para o React"""
    try:
        conn = obter_conexao()
        query = """
        SELECT
            municipio, estado, regiao, categoria_tamanho, total_anuncios_reais,
            mediana_geral, mediana_agricola, mediana_pecuaria,
            mediana_floresta_plantada, mediana_floresta_nativa,
            n_agricola, n_pecuaria, n_floresta_plantada, n_floresta_nativa,
            media_geral, desvio_padrao, coef_dispersao_pct,
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

@app.get("/api/geom")
def get_geom(tipo: str, nome: str | None = None, uf: str | None = None,
             user: str = Depends(verificar_token)):
    """Retorna GeoJSON da feature selecionada (regiao | estado | municipio)."""
    try:
        conn = obter_conexao()
        cur = conn.cursor()

        if tipo == "regiao":
            if not nome: raise HTTPException(400, "Parametro 'nome' obrigatorio")
            cur.execute("""
                SELECT ST_AsGeoJSON(ST_SimplifyPreserveTopology(mlr_geom::geometry, 0.01))
                FROM public.smt_malha_regiao WHERE lower(mlr_nm_regiao) = lower(%s)
            """, (nome,))
        elif tipo == "estado":
            if not uf: raise HTTPException(400, "Parametro 'uf' obrigatorio")
            cur.execute("""
                SELECT ST_AsGeoJSON(ST_SimplifyPreserveTopology(mlu_geom::geometry, 0.01))
                FROM public.smt_malha_uf WHERE mlu_sigla_uf = %s
            """, (uf,))
        elif tipo == "municipio":
            if not (nome and uf): raise HTTPException(400, "Parametros 'nome' e 'uf' obrigatorios")
            cur.execute("""
                SELECT ST_AsGeoJSON(ST_SimplifyPreserveTopology(mlm_geom::geometry, 0.001))
                FROM public.smt_malha_municipal
                WHERE mlm_nm_mun = %s AND mlm_sigla_uf = %s
                LIMIT 1
            """, (nome, uf))
        else:
            raise HTTPException(400, "tipo deve ser: regiao | estado | municipio")

        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row or not row[0]:
            return {"sucesso": False, "geom": None}

        return {"sucesso": True, "geom": json.loads(row[0])}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/sincronizar")
def sincronizar_view(user: str = Depends(verificar_token)):
    """Atualiza a Materialized View com novos cálculos matemáticos"""
    try:
        conn = obter_conexao()
        conn.autocommit = True 
        cur = conn.cursor()
        cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_estatisticas_simet;")
        cur.close()
        conn.close()
        return {"sucesso": True, "mensagem": "Base de dados sincronizada com sucesso!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))