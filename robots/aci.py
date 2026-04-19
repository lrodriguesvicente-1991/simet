# =====================================================================
# ARQUIVO: robots/aci.py
# MÓDULO: ACI (Auditor e Classificador Interno) - Validador de Links
# =====================================================================

import re
import requests
import time
from database.connection import obter_conexao
from database.engine import (
    obter_erros_para_auditoria,
    aprovar_reciclagem,
    descartar_link_morto
)

def verificar_status_url(url):
    """Faz um ping rápido na URL para ver se a página ainda existe.
    Detecta redirecionamentos suspeitos (para home/busca), que indicam anúncio removido."""
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
        return False, f"Falha de conexão ({str(e)[:30]})."

def executar_aci_separado(limite=50):
    print(f"[ACI] Iniciando auditoria de {limite} anúncios rejeitados...")
    conn = obter_conexao()
    if not conn: return

    erros = obter_erros_para_auditoria(conn, limite)
    if not erros:
        print("[ACI] Fila limpa! Nenhum erro encontrado para auditar.")
        conn.close()
        return

    reciclados = 0
    descartados = 0

    for err_id, fil_id, url, motivo_antigo in erros:
        print(f"Auditando ID {fil_id}...", end=" ")
        link_vivo, razao = verificar_status_url(url)
        
        if link_vivo:
            aprovar_reciclagem(conn, err_id, fil_id)
            print("[VIVO] Devolvido para PENDENTE.")
            reciclados += 1
        else:
            descartar_link_morto(conn, err_id, fil_id, razao)
            print(f"❌ MORTO. Descartado ({razao}).")
            descartados += 1
            
        time.sleep(1) 
        
    print(f"\n[ACI] RESUMO: {reciclados} reciclados | {descartados} descartados.")
    conn.close()

if __name__ == "__main__":
    executar_aci_separado()