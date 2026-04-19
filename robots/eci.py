# =====================================================================
# ARQUIVO: robots/eci.py
# MODULO: ECI (Extrator e Classificador Inteligente) — Arquitetura hibrida
#
# Divisao de responsabilidades (apos calibracao do 1o teste):
#   - AREA:       regex e primario (mais preciso). IA vira sanity-check.
#   - VALOR:      IA primario (anti-isca forte) + cross-check com DOM.
#   - UF:         subdominio da URL (rj.olx.com.br -> RJ) e autoridade.
#   - MUNICIPIO:  IA, mas UF travada pela URL para evitar alucinacao.
#   - TIPOLOGIA:  classificador de termos do DB + ids extras da IA.
#
# Pre-requisito: `ollama create simet-extrator -f Modelfile.simet-extrator`
# =====================================================================

import os
import time
import random
import re
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

import instructor
from openai import OpenAI
from pydantic import BaseModel, Field

from database.connection import obter_conexao
from database.engine import (
    obter_tarefa_da_fila, finalizar_tarefa, salvar_anuncio_final,
    salvar_tipologias, buscar_mun_id,
    extrair_area_regex_fallback, classificar_tipologias,
    listar_municipios_uf,
    detectar_isca, extrair_valor_total_regex,
)
from robots.capacidade import detectar_modo

try:
    from playwright_stealth import stealth_sync
    aplicar_stealth = stealth_sync
except ImportError:
    aplicar_stealth = None

load_dotenv()

# =====================================================================
# MODELOS PYDANTIC
# =====================================================================
class AnuncioExtrato(BaseModel):
    """Saida do agente simet-extrator. Regras em Modelfile.simet-extrator."""
    area_ha: float | str | None = Field(
        default=None,
        description="Area em hectares ja convertida. Null se nao identificada com clareza."
    )
    valor_total: float | str | None = Field(
        default=None,
        description="Valor TOTAL de venda em reais. Null se for apenas entrada/parcela/sinal."
    )
    municipio: str | None = Field(default=None, description="Nome da cidade sem UF.")
    uf: str | None = Field(default=None, description="Sigla do estado (2 letras).")
    tipologias_ids: list[int] = Field(
        default_factory=list,
        description="1=Agricola, 2=Pecuaria, 3=F.Plantada, 4=F.Nativa."
    )
    confianca: int = Field(default=0, description="0-100, quao certos estao os dados.")
    raciocinio: str = Field(default="", description="Explicacao curta de como chegou aos valores.")

# =====================================================================
# PARSERS / HELPERS
# =====================================================================
_RE_DATA_DMY = re.compile(r'^(\d{1,2})/(\d{1,2})/(\d{4})(?:\s+(\d{1,2}):(\d{2}))?')
_RE_HORA = re.compile(r'(\d{1,2}):(\d{2})')
_RE_DIA_MES = re.compile(r'(\d{1,2})/(\d{1,2})\b')
_RE_UF_URL = re.compile(r"^https?://([a-z]{2})\.olx\.com\.br/")
_RE_SLUG_URL = re.compile(r"^https?://[a-z]{2}\.olx\.com\.br/[^/]+/[^/]+/(.+?)-\d{6,}/?$")


def parse_data_publicacao(valor):
    if not valor: return None
    s = str(valor).strip()
    if not s: return None
    try:
        n = float(s)
        if n > 1e12: n = n / 1000.0
        if n < 0 or n > 4102444800: return None
        return datetime.fromtimestamp(n, tz=timezone.utc).replace(tzinfo=None)
    except (ValueError, OSError):
        pass
    try:
        dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
        if dt.tzinfo: dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        if dt.year < 2000:
            ts = (dt - datetime(1970, 1, 1)).total_seconds() * 1000
            if 0 < ts < 4102444800:
                return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        pass
    m = _RE_DATA_DMY.match(s)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)),
                            int(m.group(4) or 0), int(m.group(5) or 0))
        except ValueError:
            pass
    low = s.lower()
    hora, minuto = 0, 0
    mh = _RE_HORA.search(low)
    if mh:
        hora, minuto = int(mh.group(1)), int(mh.group(2))
    hoje = datetime.now()
    if 'hoje' in low:
        try: return hoje.replace(hour=hora, minute=minuto, second=0, microsecond=0)
        except ValueError: pass
    if 'ontem' in low:
        try:
            return (hoje - timedelta(days=1)).replace(hour=hora, minute=minuto, second=0, microsecond=0)
        except ValueError: pass
    md = _RE_DIA_MES.search(s)
    if md:
        try:
            dia, mes = int(md.group(1)), int(md.group(2))
            ano = hoje.year
            candidato = datetime(ano, mes, dia, hora, minuto)
            if candidato > hoje:
                candidato = datetime(ano - 1, mes, dia, hora, minuto)
            return candidato
        except ValueError:
            pass
    return None

def parse_uf_url(url):
    """Extrai UF do subdominio da OLX. Ex: 'https://rj.olx.com.br/...' -> 'RJ'."""
    if not url: return None
    m = _RE_UF_URL.match(url.strip())
    return m.group(1).upper() if m else None

def candidatos_municipio_url(url, uf):
    """Deriva candidatos a municipio a partir do slug da URL OLX.
    Ex: '...-niteroi-rj-1442960684' -> ['niteroi'].
    Retorna lista em ordem de especificidade (mais tokens primeiro)."""
    if not url: return []
    m = _RE_SLUG_URL.match(url.strip().lower())
    if not m: return []
    tokens = m.group(1).split("-")
    # Se ultimo token for a sigla de UF, descarta
    if tokens and uf and tokens[-1] == uf.lower():
        tokens = tokens[:-1]
    if not tokens: return []
    # Deduplica mantendo ordem; tenta 3, 2, 1 tokens
    vistos, cand = set(), []
    for n in (3, 2, 1):
        if len(tokens) >= n:
            c = " ".join(tokens[-n:])
            if c not in vistos:
                vistos.add(c); cand.append(c)
    return cand

def resolver_municipio(conn, municipio_ia, uf, url):
    """Tenta casar municipio: IA -> candidatos do slug.
    Retorna (municipio_usado, mun_id) — mun_id pode ser None."""
    mid = buscar_mun_id(conn, municipio_ia, uf) if municipio_ia else None
    if mid: return municipio_ia, mid
    for cand in candidatos_municipio_url(url, uf):
        mid = buscar_mun_id(conn, cand, uf)
        if mid:
            return cand, mid
    return municipio_ia, None

def limpar_numero(valor):
    """Converte 'R$ 1.450.000' -> 1450000.0, '324,5' -> 324.5. None se invalido."""
    if valor is None: return None
    if isinstance(valor, (int, float)): return float(valor)
    v_str = str(valor).lower().replace("r$", "").replace(" ", "")
    if "," in v_str:
        v_str = v_str.replace(".", "").replace(",", ".")
    else:
        v_str = v_str.replace(".", "")
    try:
        return float(v_str)
    except ValueError:
        return None

# =====================================================================
# JS DE COLETA: texto visivel + alguns sinais estruturados p/ cross-check
# =====================================================================
JS_COLETAR_PAGINA = r"""() => {
    let adNotFound = false;
    try {
        const lowerTitle = (document.title || "").toLowerCase();
        if (lowerTitle.includes("anuncio nao encontrado") || lowerTitle.includes("an\u00fancio n\u00e3o encontrado")) {
            adNotFound = true;
        }
    } catch(e) {}

    // Sinais estruturados do dataLayer da OLX (formulario do anunciante)
    let dlPrice = "", dlMunicipality = "", dlState = "", dlSize = "", publishDate = "";
    try {
        if (window.dataLayer && window.dataLayer.length > 0) {
            const dl = window.dataLayer[0];
            if (dl && dl.page && dl.page.adDetail) {
                const ad = dl.page.adDetail;
                if (ad.price) dlPrice = String(ad.price);
                dlMunicipality = ad.municipality || "";
                dlState = ad.state || "";
                if (ad.size) dlSize = String(ad.size);
                if (ad.adDate) publishDate = String(ad.adDate);
                if (!publishDate && ad.listTime) publishDate = String(ad.listTime);
            }
        }
    } catch(e) {}

    // Titulo: prefira H1 da pagina, fallback para document.title
    let titulo = "";
    try {
        const h1 = document.querySelector('h1');
        if (h1) titulo = (h1.innerText || "").trim();
        if (!titulo) titulo = (document.title || "").split("|")[0].trim();
    } catch(e) {}

    // Texto visivel completo (body.innerText ja pula scripts/estilos)
    let textoVisivel = "";
    try {
        textoVisivel = (document.body ? document.body.innerText : "").replace(/\s+\n/g, "\n").replace(/\n{3,}/g, "\n\n");
    } catch(e) {}

    const pageText = textoVisivel.toLowerCase();
    const bloqueado = pageText.includes("sorry") || pageText.includes("blocked") || pageText.includes("captcha");

    return {
        adNotFound, bloqueado,
        titulo,
        texto_visivel: textoVisivel.substring(0, 1500),
        // Sinais para cross-check
        dl: { dlPrice, dlMunicipality, dlState, dlSize },
        publishDate
    };
}"""

# =====================================================================
# CROSS-CHECK: compara IA vs DOM estruturado
# =====================================================================
def calcular_confianca_cross_check(ia: AnuncioExtrato, dom: dict):
    """Ajusta a confianca da IA com base em confronto com dados do dataLayer.
    Retorna (confianca_ajustada, observacoes)."""
    obs = []
    conf = ia.confianca

    dl_valor = limpar_numero(dom.get("dlPrice"))
    dl_area_m2 = limpar_numero(dom.get("dlSize"))
    dl_area_ha = dl_area_m2 * 0.0001 if dl_area_m2 else None

    ia_valor = limpar_numero(ia.valor_total)
    ia_area = limpar_numero(ia.area_ha)

    if dl_valor and ia_valor:
        diff = abs(dl_valor - ia_valor) / max(dl_valor, 1)
        if diff < 0.05:
            conf = min(100, conf + 5)
            obs.append(f"valor bate com DOM (R${dl_valor})")
        elif diff > 0.5:
            obs.append(f"valor DIVERGE do DOM (IA=R${ia_valor}, DOM=R${dl_valor})")

    if dl_area_ha and ia_area and dl_area_ha >= 1:
        diff = abs(dl_area_ha - ia_area) / max(dl_area_ha, 0.01)
        if diff < 0.1:
            conf = min(100, conf + 5)
            obs.append(f"area bate com DOM ({dl_area_ha}ha)")

    return conf, "; ".join(obs) if obs else None

def _extrair_deterministico(titulo, texto, dl, uf_url_pre):
    """Pipeline sem IA: usa dataLayer + regex + detector de isca.
    Retorna AnuncioExtrato preenchido. Tipologias vem depois via classificar_tipologias().

    Regras:
      AREA: se dl_size e regex divergem >50%, descricao manda (anunciante colocou 1 alq no form).
      VALOR: se texto indica isca E dl_price <<< regex_valor_total, dl_price e entrada.
      UF: dl_state -> fallback subdominio da URL.
      MUN: dl_municipality (validado depois pelo buscar_mun_id + slug da URL).
    """
    dl_size_m2 = limpar_numero(dl.get("dlSize"))
    dl_size_ha = round(dl_size_m2 * 0.0001, 2) if dl_size_m2 else None
    regex_area = extrair_area_regex_fallback(f"{titulo}\n{texto}")

    if dl_size_ha and regex_area:
        divergencia = abs(dl_size_ha - regex_area) / max(regex_area, 0.01)
        # divergencia grande = anunciante mentiu no form; descricao manda
        area = regex_area if divergencia > 0.5 else dl_size_ha
    else:
        area = dl_size_ha or regex_area

    dl_price = limpar_numero(dl.get("dlPrice"))
    regex_valor = extrair_valor_total_regex(texto)
    eh_isca = detectar_isca(texto)

    if eh_isca and dl_price:
        # Form diz X mas texto tem "entrada/parcela". Se regex achou valor muito maior,
        # e esse o total; senao, so temos entrada -> null.
        if regex_valor and regex_valor > dl_price * 3:
            valor = regex_valor
        else:
            valor = None
    elif dl_price:
        valor = dl_price
    elif regex_valor:
        valor = regex_valor
    else:
        valor = None

    uf = (dl.get("dlState") or "").strip().upper() or uf_url_pre
    municipio = (dl.get("dlMunicipality") or "").strip() or None

    conf = 40
    if dl_size_ha and regex_area and abs(dl_size_ha - regex_area) / max(regex_area, 0.01) < 0.1:
        conf += 25  # form e descricao concordam
    if area: conf += 10
    if valor: conf += 15
    if municipio: conf += 10
    conf = min(100, conf)

    return AnuncioExtrato(
        area_ha=area,
        valor_total=valor,
        municipio=municipio,
        uf=uf,
        tipologias_ids=[],
        confianca=conf,
        raciocinio=f"[deterministico] area={area}ha valor=R${valor} mun={municipio}",
    )


def dados_plausiveis(valor, area_ha):
    """Valida sanidade. Precisao > cobertura: dados viram teto de desapropriacao,
    outliers distorcem a mediana. Retorna (ok, motivo).
    Areas pequenas (lotes rurais) sao MANTIDAS; so descarta zero/ausente.
    Preco por hectare fora da faixa realista (R$500 a R$50M/ha) e descartado."""
    if not valor or valor < 1000:
        return False, "valor_ausente_ou_irrisorio"
    if not area_ha or area_ha <= 0:
        return False, "area_ausente"
    preco_ha = valor / area_ha
    if preco_ha < 500:
        return False, f"precoHA_baixo_demais(R${preco_ha:.0f}/ha)"
    if preco_ha > 50_000_000:
        return False, f"precoHA_alto_demais(R${preco_ha:.0f}/ha)"
    return True, None

# =====================================================================
# WORKER
# =====================================================================
def executar_eci_worker(worker_id=1, evento_lfp_fim=None, limite_isolado=None):
    info_modo = detectar_modo()
    modo = info_modo["modo"]

    cliente_ia = None
    ollama_model = None
    if modo == "ia":
        ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434/v1")
        ollama_model = os.getenv("OLLAMA_MODEL", "simet-extrator")
        cliente_ia = instructor.from_openai(
            OpenAI(base_url=ollama_url, api_key="ollama_local", timeout=180.0),
            mode=instructor.Mode.JSON,
        )
        print(f"Worker {worker_id}: modo=IA ({info_modo['motivo']}) | modelo={ollama_model}", flush=True)
    else:
        print(f"Worker {worker_id}: modo=DETERMINISTICO ({info_modo['motivo']})", flush=True)

    modo_headless = os.getenv("SIMET_HEADLESS", "0") == "1"
    delay_extra = float(os.getenv("SIMET_DELAY_EXTRA_S", "0") or 0)
    processados = 0
    esperando_log = False

    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="chrome",
            headless=modo_headless,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
        )

        while True:
            if limite_isolado and processados >= limite_isolado: break

            conn = obter_conexao()
            tarefa = obter_tarefa_da_fila(conn)
            conn.close()

            if not tarefa:
                if evento_lfp_fim and evento_lfp_fim.is_set(): break
                if not esperando_log:
                    print(f"Worker {worker_id}: [INFO] Fila vazia. Aguardando...", flush=True)
                    esperando_log = True
                time.sleep(5); continue

            esperando_log = False
            fil_id, fonte, url = tarefa
            print(f"\n{'-'*60}")
            print(f"Worker {worker_id}: [PROCESSANDO] ID {fil_id} | {url}", flush=True)

            # Amnesia: contexto novo zera cookies/fingerprint
            context = browser.new_context(
                viewport={'width': 1366, 'height': 768},
                user_agent=f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(120, 124)}.0.0.0 Safari/537.36"
            )
            page = None

            try:
                page = context.new_page()
                if aplicar_stealth: aplicar_stealth(page)

                page.goto(url, timeout=60000, wait_until="domcontentloaded")
                time.sleep(random.uniform(1.5, 2.5))
                page.keyboard.press("PageDown")
                time.sleep(random.uniform(0.5, 1.5))

                dados = page.evaluate(JS_COLETAR_PAGINA)
                print(f"Worker {worker_id}: [EXTRAINDO] ID {fil_id}", flush=True)

                # --- Triagens rapidas sem IA ---
                if dados.get('adNotFound'):
                    conn_s = obter_conexao()
                    try:
                        finalizar_tarefa(conn_s, fil_id, 'ERRO', "LINK_INATIVO: Anuncio removido")
                        print(f"Worker {worker_id}: [LINK MORTO] ID {fil_id}", flush=True)
                    finally:
                        conn_s.close()
                    processados += 1
                    continue

                if dados.get('bloqueado'):
                    print(f"Worker {worker_id}: [BLOQUEIO] Devolvendo ID {fil_id} e descansando 30s", flush=True)
                    conn_d = obter_conexao()
                    cur = conn_d.cursor()
                    cur.execute("UPDATE public.smt_fila_processamento SET fil_status = 'PENDENTE' WHERE fil_id = %s", (fil_id,))
                    conn_d.commit(); cur.close(); conn_d.close()
                    time.sleep(30)
                    continue

                titulo_pag = dados.get("titulo") or ""
                texto_pag = dados.get("texto_visivel") or ""

                if len(texto_pag) < 100:
                    conn_s = obter_conexao()
                    try:
                        finalizar_tarefa(conn_s, fil_id, 'ERRO', "DESCARTADO: pagina_sem_conteudo")
                        print(f"Worker {worker_id}: [VAZIO] ID {fil_id} -> pagina sem texto", flush=True)
                    finally:
                        conn_s.close()
                    processados += 1
                    continue

                uf_url_pre = parse_uf_url(url)
                dl = dados.get("dl", {}) or {}

                if modo == "ia":
                    # IA-first: Modelfile do agente faz extracao; injeta lista de
                    # municipios da UF para evitar alucinacao.
                    municipios_uf = []
                    if uf_url_pre:
                        conn_m = obter_conexao()
                        try:
                            municipios_uf = listar_municipios_uf(conn_m, uf_url_pre)
                        finally:
                            conn_m.close()

                    bloco_municipios = ""
                    # Guard: UFs com >400 municipios estouram o context (MG=852, SP=642, RS=497).
                    if uf_url_pre and municipios_uf and len(municipios_uf) <= 400:
                        lista_str = ", ".join(municipios_uf)
                        bloco_municipios = f"\n\nUF={uf_url_pre}. municipio DEVE ser um destes:\n{lista_str}"

                    dl_price = limpar_numero(dl.get("dlPrice"))
                    dl_size_m2 = limpar_numero(dl.get("dlSize"))
                    dl_size_ha = round(dl_size_m2 * 0.0001, 2) if dl_size_m2 else None
                    dl_mun = (dl.get("dlMunicipality") or "").strip()
                    dl_uf = (dl.get("dlState") or "").strip()

                    bloco_dom = "\n\nDADOS DO FORMULARIO (podem estar errados, valide contra DESCRICAO):"
                    bloco_dom += f"\n- area_form: {dl_size_ha} ha ({dl_size_m2} m²)" if dl_size_ha else "\n- area_form: nao informada"
                    bloco_dom += f"\n- valor_form: R$ {dl_price}" if dl_price else "\n- valor_form: nao informado"
                    bloco_dom += f"\n- municipio_form: {dl_mun}" if dl_mun else "\n- municipio_form: nao informado"
                    bloco_dom += f"\n- uf_form: {dl_uf}" if dl_uf else "\n- uf_form: nao informada"

                    prompt_usuario = (
                        f"TITULO: {titulo_pag}{bloco_dom}\n\nDESCRICAO:\n{texto_pag}{bloco_municipios}"
                    )

                    extrato = None
                    try:
                        extrato = cliente_ia.chat.completions.create(
                            model=ollama_model,
                            messages=[{"role": "user", "content": prompt_usuario}],
                            response_model=AnuncioExtrato,
                            max_retries=1,
                        )
                    except Exception as e:
                        print(f"Worker {worker_id}: [IA FALHOU] ID {fil_id} - {str(e)[:100]}", flush=True)

                    if not extrato:
                        extrato = _extrair_deterministico(titulo_pag, texto_pag, dl, uf_url_pre)
                        extrato.confianca = min(extrato.confianca, 55)
                        extrato.raciocinio = "[fallback] IA falhou; modo deterministico aplicado."
                        print(f"Worker {worker_id}: [IA->FALLBACK] ID {fil_id} usando deterministico", flush=True)
                else:
                    # Modo deterministico: dataLayer + regex + detector de isca
                    extrato = _extrair_deterministico(titulo_pag, texto_pag, dl, uf_url_pre)

                # --- Parse + cross-check ---
                tipologias = extrato.tipologias_ids or []

                conf_final, obs_cross = calcular_confianca_cross_check(extrato, dl)

                # AREA: IA primaria (le descricao completa, que e mais confiavel que formulario).
                # Regex e sanity check: se IA errou escala (>10x diff), usa regex.
                area_ia = limpar_numero(extrato.area_ha)
                area_regex = extrair_area_regex_fallback(f"{titulo_pag}\n{texto_pag}")
                if area_ia and area_regex:
                    ratio = max(area_ia, area_regex) / max(min(area_ia, area_regex), 0.01)
                    if ratio > 10:
                        print(f"Worker {worker_id}: [CORRECAO AREA] IA={area_ia}ha regex={area_regex}ha ratio={ratio:.1f}x -> usando regex (provavel erro de escala da IA)", flush=True)
                        area = area_regex
                        conf_final = min(conf_final, 60)
                    else:
                        area = area_ia
                        if abs(area_ia - area_regex) / max(area_regex, 0.01) > 0.3:
                            print(f"Worker {worker_id}: [INFO AREA] IA={area_ia}ha regex={area_regex}ha (div.>30%, IA mantida)", flush=True)
                elif area_ia:
                    area = area_ia
                elif area_regex:
                    area = area_regex
                    conf_final = min(conf_final, 50)
                    print(f"Worker {worker_id}: [AREA REGEX] {area}ha (IA nao extraiu)", flush=True)
                else:
                    area = None

                # VALOR: IA primario (anti-isca). DOM como fallback / corretor de escala.
                # Se IA e DOM divergem >5x, IA provavelmente errou escala (ex: leu 8M ao inves de 800k).
                valor_ia = limpar_numero(extrato.valor_total)
                dl_valor = limpar_numero(dl.get("dlPrice"))
                if valor_ia and dl_valor:
                    ratio = max(valor_ia, dl_valor) / max(min(valor_ia, dl_valor), 1)
                    if ratio > 5:
                        print(f"Worker {worker_id}: [CORRECAO VALOR] IA=R${valor_ia} DOM=R${dl_valor} ratio={ratio:.1f}x -> usando DOM", flush=True)
                        valor = dl_valor
                    else:
                        valor = valor_ia
                elif valor_ia:
                    valor = valor_ia
                elif dl_valor:
                    valor = dl_valor
                    conf_final = min(conf_final, 45)
                    print(f"Worker {worker_id}: [VALOR DOM] R${valor} (IA nao extraiu)", flush=True)
                else:
                    valor = None

                # UF: subdominio da URL e autoridade. IA pode completar nome da cidade.
                uf_ia = (extrato.uf or "").strip().upper() or None
                if uf_url_pre and uf_ia and uf_url_pre != uf_ia:
                    print(f"Worker {worker_id}: [ALERTA UF] URL={uf_url_pre} vs IA={uf_ia} -> usando URL", flush=True)
                uf = uf_url_pre or uf_ia
                municipio = (extrato.municipio or "").strip() or None

                # --- Validacoes finais ---
                if conf_final < 50:
                    conn_s = obter_conexao()
                    try:
                        finalizar_tarefa(conn_s, fil_id, 'ERRO',
                                          f"DESCARTADO: confianca_baixa({conf_final}) | {extrato.raciocinio[:100]}")
                        print(f"Worker {worker_id}: [CONFIANCA BAIXA] ID {fil_id} ({conf_final}) -> {extrato.raciocinio[:80]}", flush=True)
                    finally:
                        conn_s.close()
                    processados += 1
                    continue

                ok, motivo = dados_plausiveis(valor, area)
                if not ok:
                    conn_s = obter_conexao()
                    try:
                        finalizar_tarefa(conn_s, fil_id, 'ERRO', f"DESCARTADO: {motivo}")
                        print(f"Worker {worker_id}: [IMPLAUSIVEL] ID {fil_id} -> {motivo}", flush=True)
                    finally:
                        conn_s.close()
                    processados += 1
                    continue

                # --- Salvamento ---
                data_pub_texto = dados.get("publishDate") or None
                data_pub_dt = parse_data_publicacao(data_pub_texto)

                conn_s = obter_conexao()
                try:
                    municipio_final, mun_id_db = resolver_municipio(conn_s, municipio, uf, url)
                    if municipio_final != municipio and mun_id_db:
                        print(f"Worker {worker_id}: [MUN VIA SLUG] IA={municipio} -> URL={municipio_final}", flush=True)
                    if not mun_id_db:
                        finalizar_tarefa(conn_s, fil_id, 'ERRO',
                                          f"Municipio nao encontrado: {municipio}/{uf}")
                        print(f"Worker {worker_id}: [MUN NAO ENCONTRADO] ID {fil_id} -> {municipio}/{uf}", flush=True)
                    else:
                        print(f"Worker {worker_id}: [SALVANDO] ID {fil_id} | {municipio_final}/{uf}", flush=True)
                        anc_id = salvar_anuncio_final(conn_s, fil_id, {
                            'origem': fonte, 'link': url, 'titulo': titulo_pag,
                            'desc': texto_pag,
                            'preco_real': valor, 'area_ha': area,
                            'mun': municipio_final, 'uf': uf, 'mun_id': mun_id_db,
                            'data_publicacao_texto': data_pub_texto,
                            'data_publicacao': data_pub_dt,
                        })
                        # Tipologia: une IA + classificador de termos
                        tip_termos = classificar_tipologias(conn_s, f"{titulo_pag} {texto_pag}")
                        tip_final = list({*tip_termos, *tipologias})
                        if tip_final: salvar_tipologias(conn_s, anc_id, tip_final)

                        finalizar_tarefa(conn_s, fil_id, 'CONCLUIDO')
                        extras = f" | cross:{obs_cross}" if obs_cross else ""
                        print(f"Worker {worker_id}: [SUCESSO] ID {fil_id} | {area}ha | R${valor} | "
                              f"{municipio_final}/{uf} | Tip:{tip_final} | conf:{conf_final}{extras}", flush=True)
                        if extrato.raciocinio:
                            print(f"  raciocinio: {extrato.raciocinio[:180]}", flush=True)
                finally:
                    conn_s.close()

            except Exception as e:
                print(f"Worker {worker_id}: [ERRO CRITICO] ID {fil_id} - {str(e)[:120]}", flush=True)
                try:
                    conn_e = obter_conexao()
                    finalizar_tarefa(conn_e, fil_id, 'ERRO', str(e)[:60])
                    conn_e.close()
                except Exception:
                    pass
            finally:
                if page:
                    try: page.close()
                    except Exception: pass
                if context:
                    try: context.close()
                    except Exception: pass
                processados += 1
                if delay_extra > 0:
                    time.sleep(delay_extra)

        browser.close()

