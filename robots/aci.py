# =====================================================================
# ARQUIVO: robots/aci.py
# MODULO: ACI (Auditor e Classificador Interno) - Reciclador da fila de erros
#
# Fluxo:
#   1. Para cada anuncio em ERRO, verifica se a URL ainda esta viva
#   2. Se morta (404, redirect generico)                 -> LINK_INATIVO (descartado)
#   3. Se viva, classifica pelo err_motivo:
#        MUN_NAO_ENCONTRADO        -> PENDENTE     (parser corrigido, retry determinstico)
#        VAZIO / IMPLAUSIVEL       -> PENDENTE_IA  (so IA tenta)
#        CONFIANCA_BAIXA           -> REJEITADO_IA (IA ja falhou, so humano)
#        demais (timeout, critico) -> PENDENTE     (transitorio, retry normal)
# =====================================================================

import re
import time

import requests

from database.connection import obter_conexao
from database.engine import (
    aprovar_reciclagem,
    descartar_link_morto,
    obter_erros_para_auditoria,
    rejeitar_ia_direto,
)
from robots._controle import foi_solicitado_parar


def verificar_status_url(url):
    """Faz um ping rapido na URL para ver se a pagina ainda existe.
    Detecta redirecionamentos suspeitos (para home/busca), que indicam anuncio removido."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0 Safari/537.36'
    }
    try:
        resposta = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
        if resposta.status_code in [404, 410]:
            return False, f"HTTP {resposta.status_code}: Removido."

        # Redirecionamento para home/listagem = anuncio removido.
        # URLs de detalhe na OLX terminam com um id numerico (8+ digitos).
        # Se o destino final nao tem esse id, virou pagina generica.
        if resposta.url != url and not re.search(r'\d{8,}(?:/|$|\?)', resposta.url):
            return False, f"Redirecionado para pagina generica: {resposta.url[:60]}"

        return True, "URL Ativa."
    except requests.RequestException as e:
        return False, f"Falha de conexao ({str(e)[:30]})."


def classificar_destino(motivo):
    """Decide o destino de um anuncio vivo com base no err_motivo.

    Retorna uma tupla (acao, novo_status_ou_none), onde acao e:
      'reciclar'    -> volta pra fila no status indicado (PENDENTE / PENDENTE_IA)
      'rejeitar_ia' -> REJEITADO_IA direto, nao volta pra fila
    """
    texto = (motivo or "").lower()

    # IA ja tentou e nao teve certeza -> nao adianta rodar de novo
    if "confianca_baixa" in texto or "confianca baixa" in texto:
        return ("rejeitar_ia", None)

    # Parser de municipio foi corrigido, retry determinstico
    if "municipio nao encontrado" in texto or "mun_nao_encontrado" in texto:
        return ("reciclar", "PENDENTE")

    # Erros estruturais: so IA tem chance de resolver
    if "vazio" in texto or "implausivel" in texto or "implausível" in texto:
        return ("reciclar", "PENDENTE_IA")

    # Transitorio (timeout, erro critico sem categoria) -> retry normal
    return ("reciclar", "PENDENTE")


def executar_aci_separado(limite=None):
    """Audita anuncios em ERRO. limite=None/<=0 -> processa toda a fila."""
    conn = obter_conexao()
    if not conn:
        return

    erros = obter_erros_para_auditoria(conn, limite)
    if not erros:
        print("[ACI] Fila limpa! Nenhum erro encontrado para auditar.", flush=True)
        conn.close()
        return

    # Print do "Iniciando" SAI APOS o fetch para usar a contagem real:
    # garante que a barra de progresso no front (totalAuditar) reflita
    # exatamente quantos anuncios serao processados, com ou sem limite.
    print(f"[ACI] Iniciando auditoria de {len(erros)} anuncios rejeitados...", flush=True)

    reciclados_normal = 0
    reciclados_ia = 0
    rejeitados_ia = 0
    descartados = 0

    for err_id, fil_id, url, motivo_antigo in erros:
        if foi_solicitado_parar():
            print("[ACI] Parando auditoria após o item atual...", flush=True)
            break

        print(f"[ACI] Auditando ID {fil_id} (motivo: {str(motivo_antigo)[:50]})...", flush=True)
        link_vivo, razao = verificar_status_url(url)

        if not link_vivo:
            descartar_link_morto(conn, err_id, fil_id, razao)
            print(f"[ACI]   MORTO -> descartado #{fil_id} ({razao})", flush=True)
            descartados += 1
            time.sleep(1)
            continue

        acao, destino = classificar_destino(motivo_antigo)
        if acao == "rejeitar_ia":
            rejeitar_ia_direto(conn, err_id, fil_id, motivo_antigo)
            print(f"[ACI]   VIVO + IA ja falhou -> REJEITADO_IA #{fil_id} (revisao humana)", flush=True)
            rejeitados_ia += 1
        else:
            aprovar_reciclagem(conn, err_id, fil_id, status_destino=destino)
            if destino == "PENDENTE_IA":
                print(f"[ACI]   VIVO -> PENDENTE_IA #{fil_id} (fila exclusiva de GPU)", flush=True)
                reciclados_ia += 1
            else:
                print(f"[ACI]   VIVO -> PENDENTE #{fil_id} (retry)", flush=True)
                reciclados_normal += 1

        time.sleep(1)

    print(
        f"\n[ACI] RESUMO: {reciclados_normal} -> PENDENTE | "
        f"{reciclados_ia} -> PENDENTE_IA | "
        f"{rejeitados_ia} -> REJEITADO_IA | "
        f"{descartados} descartados.",
        flush=True,
    )
    conn.close()


if __name__ == "__main__":
    executar_aci_separado()
