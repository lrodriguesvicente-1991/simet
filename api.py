# =====================================================================
# ARQUIVO: api.py
# DESCRIÇÃO: API RESTful (FastAPI) Segura com JWT e Leitura de Pipe
# =====================================================================

import io
import json
import os
import sys
from collections import deque
import psutil
import subprocess
import threading
import jwt
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import pandas as pd
from openpyxl.utils import get_column_letter
from dotenv import load_dotenv

from database.connection import obter_conexao
from database import usuarios as usuarios_db

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

try:
    usuarios_db.garantir_admin_seed()
except Exception as _seed_err:
    print(f"[SEED WARN] Falha ao garantir admin inicial: {_seed_err}", flush=True)


@app.post("/api/login")
def fazer_login(req: LoginRequest):
    user = usuarios_db.buscar_por_username(req.usuario)
    if not user or not user.get("ativo") or not usuarios_db.verificar_senha(req.senha, user["senha_hash"]):
        return {"sucesso": False, "mensagem": "Credenciais invalidas"}

    payload = {
        "sub": user["username"],
        "nivel": user["nivel"],
        "uid": user["id"],
        "exp": datetime.now(timezone.utc) + timedelta(hours=24),
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    return {"sucesso": True, "token": token, "nivel": user["nivel"], "usuario": user["username"]}


def verificar_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        return {
            "username": payload["sub"],
            "nivel": int(payload.get("nivel", 3)),
            "id": payload.get("uid"),
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token invalido")


def exigir_nivel(nivel_maximo: int):
    """Depende de verificar_token e barra usuarios com nivel MAIOR que o permitido.
    Lembre-se: 0=Admin (mais poder), 3=Visualizador (menos poder)."""
    def _dep(user: dict = Depends(verificar_token)) -> dict:
        if user["nivel"] > nivel_maximo:
            raise HTTPException(status_code=403, detail="Permissao insuficiente para esta acao.")
        return user
    return _dep

# =====================================================================
# GERENCIAMENTO DE ESTADO E PROCESSOS (Com Threading)
# =====================================================================
CB_MARKER = "[CIRCUIT_BREAKER]"

estado_robo = {
    "rodando": False,
    "processo": None, # Guarda o objeto Popen
    "logs": deque(maxlen=500),
    "circuit_breaker": False,
    "circuit_breaker_msg": None,
}

def ler_logs_robo(proc):
    """Lê os logs do stdout em tempo real numa Thread separada (Evita block do OS)"""
    for linha in iter(proc.stdout.readline, ''):
        if linha:
            linha_strip = linha.strip()
            estado_robo["logs"].append(linha_strip)
            if CB_MARKER in linha_strip:
                estado_robo["circuit_breaker"] = True
                estado_robo["circuit_breaker_msg"] = linha_strip
    proc.stdout.close()

class RoboRequest(BaseModel):
    task: str
    workers: int = 1
    limit: int = 0
    headless: bool = True
    delay_extra: float = 0.0

@app.post("/api/robo/iniciar")
def iniciar_robo(req: RoboRequest, user: dict = Depends(exigir_nivel(1))):
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
        estado_robo["logs"] = deque([f"[SYSTEM] Iniciando operação: {req.task.upper()}..."], maxlen=500)
        estado_robo["circuit_breaker"] = False
        estado_robo["circuit_breaker_msg"] = None
        
        # Dispara a thread para não bloquear o pipe!
        t = threading.Thread(target=ler_logs_robo, args=(processo,), daemon=True)
        t.start()
        
        return {"sucesso": True, "mensagem": "Robôs iniciados!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao iniciar robô: {e}")

@app.post("/api/robo/parar")
def parar_robo(user: dict = Depends(exigir_nivel(1))):
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
def status_robo(user: dict = Depends(exigir_nivel(2))):
    # Checagem de vida: O processo morreu naturalmente (terminou o limite)?
    if estado_robo["rodando"] and estado_robo["processo"]:
        if estado_robo["processo"].poll() is not None: # != None significa que acabou
            estado_robo["rodando"] = False
            estado_robo["processo"] = None
            estado_robo["logs"].append("[SYSTEM] Processo finalizado naturalmente.")

    logs = estado_robo["logs"]
    ultimas = list(logs)[-50:] if logs else []
    return {
        "rodando": estado_robo["rodando"],
        "logs_recentes": ultimas,
        "circuit_breaker": estado_robo["circuit_breaker"],
        "circuit_breaker_msg": estado_robo["circuit_breaker_msg"],
    }

@app.post("/api/robo/reconhecer-alarme")
def reconhecer_alarme(user: dict = Depends(exigir_nivel(1))):
    """Administrador reconhece o alarme do circuit breaker e limpa a flag."""
    estado_robo["circuit_breaker"] = False
    estado_robo["circuit_breaker_msg"] = None
    return {"sucesso": True}

# =====================================================================
# ROTAS DE DASHBOARD (Requerem Token)
# =====================================================================
@app.get("/api/dados")
def get_dashboard_data(user: dict = Depends(verificar_token)):
    """Busca os dados mastigados da View e converte para JSON para o React."""
    try:
        conn = obter_conexao()
        query = """
        SELECT
            v.municipio, v.estado, v.regiao, v.categoria_tamanho, v.total_anuncios_reais,
            v.mediana_geral, v.mediana_agricola, v.mediana_pecuaria,
            v.mediana_floresta_plantada, v.mediana_floresta_nativa,
            v.n_agricola, v.n_pecuaria, v.n_floresta_plantada, v.n_floresta_nativa,
            v.media_geral, v.media_agricola, v.media_pecuaria,
            v.media_floresta_plantada, v.media_floresta_nativa,
            v.desvio_padrao, v.coef_dispersao_pct,
            mr.mre_codigo AS mercado_regional_codigo,
            mr.mre_nome   AS mercado_regional_nome,
            ST_Y(ST_Centroid(v.geom_municipio::geometry)) AS lat,
            ST_X(ST_Centroid(v.geom_municipio::geometry)) AS lon
        FROM public.vw_media_mercado_terras v
        LEFT JOIN public.smt_unidade_federativa u ON u.unf_sigla = v.estado
        LEFT JOIN public.smt_municipio m
               ON m.mun_nome = v.municipio AND m.mun_unf_id = u.unf_id
        LEFT JOIN public.smt_mercado_regional mr ON mr.mre_id = m.mun_mre_id
        WHERE v.geom_municipio IS NOT NULL
        """
        df = pd.read_sql(query, conn)
        conn.close()
        df = df.replace({pd.NA: None, float('nan'): None})
        return {"sucesso": True, "dados": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/mercados-regionais")
def listar_mercados_regionais(user: dict = Depends(verificar_token)):
    """Lista os mercados regionais (tipo smt_mercado_regional) com a UF inferida do codigo."""
    try:
        conn = obter_conexao()
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT mr.mre_codigo, mr.mre_nome, u.unf_sigla
            FROM public.smt_mercado_regional mr
            LEFT JOIN public.smt_municipio m ON m.mun_mre_id = mr.mre_id
            LEFT JOIN public.smt_unidade_federativa u ON u.unf_id = m.mun_unf_id
            ORDER BY u.unf_sigla NULLS LAST, mr.mre_nome
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {
            "sucesso": True,
            "mercados": [
                {"codigo": r[0], "nome": r[1], "uf": (r[2] or "").strip() or None}
                for r in rows
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/geom")
def get_geom(tipo: str, nome: str | None = None, uf: str | None = None,
             user: dict = Depends(verificar_token)):
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
def sincronizar_view(user: dict = Depends(exigir_nivel(1))):
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

# =====================================================================
# EXPORTACAO DE RELATORIOS (XLSX e PDF)
# =====================================================================
def _buscar_dados_relatorio(regiao=None, estado=None, categoria=None,
                            municipio=None, mercado_regional=None):
    """Busca a view aplicando os mesmos filtros do Observatorio (SQL-side)."""
    wheres: list[str] = ["v.geom_municipio IS NOT NULL"]
    params: list = []
    if regiao and regiao.lower() not in ("", "todas"):
        wheres.append("v.regiao = %s"); params.append(regiao)
    if estado and estado.lower() not in ("", "todos"):
        wheres.append("v.estado = %s"); params.append(estado)
    if categoria and categoria.lower() not in ("", "todos"):
        cats = [c.strip() for c in categoria.split(",") if c.strip()]
        if len(cats) == 1:
            wheres.append("v.categoria_tamanho = %s"); params.append(cats[0])
        elif len(cats) > 1:
            placeholders = ", ".join(["%s"] * len(cats))
            wheres.append(f"v.categoria_tamanho IN ({placeholders})")
            params.extend(cats)
    if municipio and municipio.lower() not in ("", "todos"):
        wheres.append("v.municipio = %s"); params.append(municipio)
    if mercado_regional and mercado_regional.lower() not in ("", "todos"):
        wheres.append("mr.mre_codigo = %s"); params.append(mercado_regional)

    query = """
    SELECT v.municipio, v.estado, v.regiao, v.categoria_tamanho, v.total_anuncios_reais,
           v.mediana_geral, v.mediana_agricola, v.mediana_pecuaria,
           v.mediana_floresta_plantada, v.mediana_floresta_nativa,
           v.n_agricola, v.n_pecuaria, v.n_floresta_plantada, v.n_floresta_nativa,
           v.media_geral, v.media_agricola, v.media_pecuaria,
           v.media_floresta_plantada, v.media_floresta_nativa,
           v.desvio_padrao, v.coef_dispersao_pct,
           mr.mre_codigo AS mercado_regional_codigo,
           mr.mre_nome   AS mercado_regional_nome
    FROM public.vw_media_mercado_terras v
    LEFT JOIN public.smt_unidade_federativa u ON u.unf_sigla = v.estado
    LEFT JOIN public.smt_municipio m
           ON m.mun_nome = v.municipio AND m.mun_unf_id = u.unf_id
    LEFT JOIN public.smt_mercado_regional mr ON mr.mre_id = m.mun_mre_id
    WHERE """ + " AND ".join(wheres)

    conn = obter_conexao()
    try:
        df = pd.read_sql(query, conn, params=params or None)
    finally:
        conn.close()
    return df.replace({pd.NA: None, float('nan'): None})


@app.get("/api/relatorio/xlsx")
def exportar_xlsx(regiao: str | None = None, estado: str | None = None,
                  categoria: str | None = None, municipio: str | None = None,
                  mercado_regional: str | None = None,
                  user: dict = Depends(verificar_token)):
    """Exporta os dados filtrados em XLSX (sem limite de linhas)."""
    try:
        df = _buscar_dados_relatorio(regiao, estado, categoria, municipio, mercado_regional)

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="SIMET")
            ws = writer.sheets["SIMET"]
            for col_idx, col_name in enumerate(df.columns, start=1):
                largura = max(12, min(30, len(str(col_name)) + 2))
                ws.column_dimensions[get_column_letter(col_idx)].width = largura

        buffer.seek(0)
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        nome = f"simet_{ts}.xlsx"
        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{nome}"'},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


_FAVICON_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "frontend_react", "public", "favicon.png")


def _desenhar_logo_incra(c, x, y, tamanho=36):
    """Desenha o logotipo do INCRA a partir do favicon.png.
    Se o arquivo nao estiver disponivel, cai para o logotipo vetorial."""
    from reportlab.lib.colors import HexColor
    from reportlab.lib.utils import ImageReader

    if os.path.exists(_FAVICON_PATH):
        try:
            c.drawImage(ImageReader(_FAVICON_PATH), x, y,
                        width=tamanho, height=tamanho,
                        mask='auto', preserveAspectRatio=True)
            return
        except Exception:
            pass  # fallback vetorial abaixo

    verde = HexColor("#6FA030")
    t = tamanho
    c.setFillColor(verde)
    q = t * 0.18
    gap = t * 0.02
    ox = x + t * 0.34
    oy = y + t * 0.55
    for i in range(2):
        for j in range(2):
            c.rect(ox + i * (q + gap), oy + (1 - j) * (q + gap), q, q, fill=1, stroke=0)
    c.setFillColor(verde)
    p = c.beginPath(); p.moveTo(x, y + t * 0.05)
    p.lineTo(x + t * 0.44, y + t * 0.12); p.lineTo(x + t * 0.46, y + t * 0.55)
    p.lineTo(x, y + t * 0.5); p.close(); c.drawPath(p, fill=1, stroke=0)
    p = c.beginPath(); p.moveTo(x + t * 0.54, y + t * 0.12)
    p.lineTo(x + t, y + t * 0.05); p.lineTo(x + t, y + t * 0.5)
    p.lineTo(x + t * 0.52, y + t * 0.55); p.close(); c.drawPath(p, fill=1, stroke=0)


def _fmt_brl(v):
    if v is None or (isinstance(v, float) and (pd.isna(v) or not v)):
        return "—"
    try:
        return f"R$ {float(v):,.0f}".replace(",", ".")
    except Exception:
        return "—"


@app.get("/api/relatorio/pdf")
def exportar_pdf(estado: str, categoria: str | None = None,
                 mercado_regional: str | None = None,
                 user: dict = Depends(verificar_token)):
    """Exporta relatorio PDF para uma UF especifica."""
    if not estado or estado.lower() == "todos":
        raise HTTPException(status_code=400, detail="Parametro 'estado' (UF) e obrigatorio no PDF.")

    try:
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib.colors import HexColor, black
        from reportlab.pdfgen import canvas
        from reportlab.platypus import Table, TableStyle
        from reportlab.lib import colors as rl_colors

        df = _buscar_dados_relatorio(regiao=None, estado=estado,
                                     categoria=categoria, mercado_regional=mercado_regional)
        df = df.sort_values(["municipio", "categoria_tamanho"], na_position="last")

        mercado_nome = ""
        if mercado_regional and mercado_regional.lower() != "todos" and not df.empty:
            nome_col = df["mercado_regional_nome"].dropna()
            if not nome_col.empty:
                mercado_nome = str(nome_col.iloc[0])

        buffer = io.BytesIO()
        largura, altura = landscape(A4)
        c = canvas.Canvas(buffer, pagesize=landscape(A4))

        def cabecalho():
            _desenhar_logo_incra(c, 36, altura - 60, tamanho=40)
            c.setFillColor(HexColor("#3D6A1C"))
            c.setFont("Helvetica-Bold", 18)
            c.drawString(90, altura - 42, "SIMET · Observatorio de Mercado de Terras")
            c.setFillColor(black)
            c.setFont("Helvetica", 10)
            sub = f"UF: {estado}"
            if mercado_nome:
                sub += f" · Mercado Regional: {mercado_nome}"
            if categoria and categoria.lower() != "todos":
                cats_txt = ", ".join(c.strip() for c in categoria.split(",") if c.strip())
                if cats_txt:
                    sub += f" · Categoria: {cats_txt}"
            sub += f" · Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}"
            c.drawString(90, altura - 58, sub)
            c.setStrokeColor(HexColor("#6FA030"))
            c.setLineWidth(1.2)
            c.line(36, altura - 70, largura - 36, altura - 70)

        def rodape(pagina, total):
            c.setFont("Helvetica-Oblique", 8)
            c.setFillColor(HexColor("#888888"))
            c.drawString(36, 20, "SIMET · Sistema de Inteligencia de Mercado de Terras · INCRA")
            c.drawRightString(largura - 36, 20, f"Pagina {pagina}/{total}")
            c.setFillColor(black)

        if df.empty:
            cabecalho()
            c.setFont("Helvetica", 12)
            c.drawString(40, altura - 100, "Nenhum dado encontrado para os filtros selecionados.")
            rodape(1, 1)
            c.save()
        else:
            cabecalhos = ["Municipio", "Mercado Regional", "Categoria", "Mediana Geral", "Amostras", "Media Geral", "Disp. %"]
            linhas = [cabecalhos]
            for _, r in df.iterrows():
                linhas.append([
                    str(r["municipio"]),
                    str(r.get("mercado_regional_nome") or "—"),
                    str(r["categoria_tamanho"]),
                    _fmt_brl(r.get("mediana_geral")),
                    str(int(r.get("total_anuncios_reais") or 0)),
                    _fmt_brl(r.get("media_geral")),
                    f"{float(r['coef_dispersao_pct']):.1f}%" if r.get("coef_dispersao_pct") is not None else "—",
                ])

            chunk = 22
            total_pags = max(1, (len(linhas) - 1 + chunk - 1) // chunk)
            for p in range(total_pags):
                cabecalho()
                inicio = 1 + p * chunk
                fim = min(len(linhas), inicio + chunk)
                trecho = [cabecalhos] + linhas[inicio:fim]
                tbl = Table(trecho, colWidths=[120, 140, 110, 90, 60, 90, 55])
                tbl.setStyle(TableStyle([
                    ("BACKGROUND", (0, 0), (-1, 0), HexColor("#3D6A1C")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), rl_colors.white),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [rl_colors.whitesmoke, rl_colors.white]),
                    ("GRID", (0, 0), (-1, -1), 0.25, HexColor("#CCCCCC")),
                    ("ALIGN", (3, 1), (-1, -1), "RIGHT"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ]))
                w, h = tbl.wrapOn(c, largura - 72, altura - 140)
                tbl.drawOn(c, 36, altura - 80 - h)
                rodape(p + 1, total_pags)
                if p + 1 < total_pags:
                    c.showPage()
            c.save()

        buffer.seek(0)
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        nome = f"simet_{estado}_{ts}.pdf"
        return StreamingResponse(
            buffer,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{nome}"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# GESTAO DE USUARIOS (nivel 0 apenas, exceto /me)
# =====================================================================
class NovoUsuarioReq(BaseModel):
    usuario: str
    senha: str
    nivel: int

class AtualizarUsuarioReq(BaseModel):
    nivel: int | None = None
    ativo: bool | None = None

class ResetSenhaReq(BaseModel):
    nova_senha: str


@app.get("/api/me")
def obter_perfil_atual(user: dict = Depends(verificar_token)):
    return {"usuario": user["username"], "nivel": user["nivel"], "id": user.get("id")}


@app.get("/api/usuarios")
def listar_usuarios(user: dict = Depends(exigir_nivel(0))):
    return {"sucesso": True, "usuarios": usuarios_db.listar()}


@app.post("/api/usuarios")
def criar_usuario(req: NovoUsuarioReq, user: dict = Depends(exigir_nivel(0))):
    try:
        novo_id = usuarios_db.criar(req.usuario.strip(), req.senha, int(req.nivel))
        return {"sucesso": True, "id": novo_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Nao foi possivel criar usuario: {e}")


@app.patch("/api/usuarios/{usr_id}")
def atualizar_usuario(usr_id: int, req: AtualizarUsuarioReq,
                      user: dict = Depends(exigir_nivel(0))):
    if usr_id == user.get("id"):
        if req.ativo is False:
            raise HTTPException(status_code=400, detail="Voce nao pode desativar a si mesmo.")
        if req.nivel is not None and req.nivel != 0:
            raise HTTPException(status_code=400, detail="Voce nao pode rebaixar a si mesmo.")

    if (req.nivel is not None and req.nivel != 0) or req.ativo is False:
        alvo = usuarios_db.buscar_por_id(usr_id)
        if alvo and alvo["nivel"] == 0 and alvo["ativo"]:
            if usuarios_db.contar_admins_ativos() <= 1:
                raise HTTPException(
                    status_code=400,
                    detail="Nao e possivel remover o poder do ultimo administrador ativo.",
                )

    try:
        usuarios_db.atualizar(usr_id, nivel=req.nivel, ativo=req.ativo)
        return {"sucesso": True}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/usuarios/{usr_id}/reset-senha")
def resetar_senha_usuario(usr_id: int, req: ResetSenhaReq,
                          user: dict = Depends(exigir_nivel(0))):
    try:
        usuarios_db.resetar_senha(usr_id, req.nova_senha)
        return {"sucesso": True}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/usuarios/{usr_id}")
def excluir_usuario(usr_id: int, user: dict = Depends(exigir_nivel(0))):
    if usr_id == user.get("id"):
        raise HTTPException(status_code=400, detail="Voce nao pode excluir a si mesmo.")
    alvo = usuarios_db.buscar_por_id(usr_id)
    if alvo and alvo["nivel"] == 0 and alvo["ativo"]:
        if usuarios_db.contar_admins_ativos() <= 1:
            raise HTTPException(
                status_code=400,
                detail="Nao e possivel excluir o ultimo administrador ativo.",
            )
    usuarios_db.excluir(usr_id)
    return {"sucesso": True}