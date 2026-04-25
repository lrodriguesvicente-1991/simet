"""Controle graceful de parada dos robos.

A API sinaliza a parada criando o arquivo .robo_stop na raiz do projeto.
Cada worker checa a existencia desse arquivo entre iteracoes e sai limpo
apos concluir o anuncio atual -- sem abortar o Chromium no meio da
extracao, sem criar anuncios parcialmente salvos, sem mexer com SIGTERM
em subprocessos do Playwright.

Se a parada graciosa nao ocorrer dentro do timeout da API, ela cai para
taskkill /F /T (fallback).
"""
import os

STOP_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", ".robo_stop"
)


def foi_solicitado_parar() -> bool:
    """True se a API pediu parada graciosa."""
    return os.path.exists(STOP_FILE)


def marcar_parada() -> None:
    """Cria o arquivo sinalizador. Chamado pela API."""
    try:
        with open(STOP_FILE, "w", encoding="utf-8") as f:
            f.write("stop")
    except OSError:
        pass


def limpar_parada() -> None:
    """Remove o arquivo sinalizador. Chamado no inicio de cada operacao
    e apos o processo terminar."""
    try:
        if os.path.exists(STOP_FILE):
            os.remove(STOP_FILE)
    except OSError:
        pass
