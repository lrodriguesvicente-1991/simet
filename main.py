# =====================================================================
# ARQUIVO: main.py
# MÓDULO: Orquestrador / CLI Master
# DESCRIÇÃO: Controla a execução concorrente dos módulos LFP e ECI.
# Totalmente preparado para ser chamado via N8N (argparse) ou Interface.
# =====================================================================

import os
import sys
import argparse
import multiprocessing
from dotenv import load_dotenv

from robots.lfp import executar_lfp
from robots.eci import executar_eci_worker
from robots.aci import executar_aci_separado
from database.connection import obter_conexao
from database.engine import obter_tarefas_ativas

load_dotenv()

def testar_conexao_banco():
    print("[SYSTEM] Testando conexão com o PostGIS...", flush=True)
    try:
        conn = obter_conexao()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print(f"[SYSTEM] Conexão bem-sucedida! Versão: {db_version[0][:40]}", flush=True)
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"[SYSTEM] Falha na conexão: {e}", flush=True)

def iniciar_pipeline_v2(num_workers):
    tarefas = obter_tarefas_ativas()
    if not tarefas:
        print("[SYSTEM] Sem configurações ativas no banco.", flush=True)
        return
        
    evento_lfp_fim = multiprocessing.Event()

    proc_lfp = multiprocessing.Process(target=executar_lfp, args=(tarefas, evento_lfp_fim))
    workers = []
    
    for i in range(num_workers):
        p = multiprocessing.Process(target=executar_eci_worker, args=(i+1, evento_lfp_fim, None))
        workers.append(p)
        
    proc_lfp.start()
    for w in workers: w.start()
        
    proc_lfp.join()
    for w in workers: w.join()
    
    print("[SYSTEM] Operação FULL Concluída!", flush=True)

def executar_tarefa(tarefa_id, num_workers=1, limite=0):
    if tarefa_id == "1" or tarefa_id == "lfp":
        tarefas = obter_tarefas_ativas()
        executar_lfp(tarefas)
    elif tarefa_id == "2" or tarefa_id == "eci":
        limit_val = limite if limite > 0 else None
        evento_fim = multiprocessing.Event()
        workers_list = []
        for i in range(num_workers):
            p = multiprocessing.Process(target=executar_eci_worker, args=(i+1, evento_fim, limit_val))
            workers_list.append(p)
        for w in workers_list: w.start()
        for w in workers_list: w.join()
            
    elif tarefa_id == "3" or tarefa_id == "aci":
        limit_val = limite if limite > 0 else 50
        executar_aci_separado(limite=limit_val)
        
    elif tarefa_id == "4" or tarefa_id == "full":
        iniciar_pipeline_v2(num_workers)
    elif tarefa_id == "5" or tarefa_id == "test":
        testar_conexao_banco()

def interacao_terminal():
    """Modo desenvolvedor manual caso rode o main.py sem argumentos"""
    while True:
        print("\n--- SIMET V2 ---")
        print("1. LFP Isolado")
        print("2. ECI Isolado")
        print("3. ACI Isolado (Auditoria)")
        print("4. FULL (Pipeline Concorrente)")
        print("5. Teste de Conexão")
        print("0. Sair")
        
        opcao = input("\nEscolha a opção: ").strip()
        if opcao == "0": break
        
        workers = 1
        limite = 0
        if opcao in ["2", "4"]:
            w_input = input("Quantos Workers? (Padrão 1): ").strip()
            workers = int(w_input) if w_input.isdigit() else 1
        if opcao in ["2", "3"]:
            l_input = input("Limite de processamento? (0 para padrão): ").strip()
            limite = int(l_input) if l_input.isdigit() else 0
                
        executar_tarefa(opcao, workers, limite)

def main():
    multiprocessing.freeze_support()
    
    parser = argparse.ArgumentParser(description="SIMET V2 Orchestrator")
    parser.add_argument('--task', type=str, choices=['lfp', 'eci', 'aci', 'full', 'test'], help="Tarefa a executar")
    parser.add_argument('--workers', type=int, default=1, help="Número de workers (Apenas para FULL ou ECI)")
    parser.add_argument('--limit', type=int, default=0, help="Limite de anúncios (Apenas ECI e ACI)")
    
    args = parser.parse_args()

    if args.task:
        executar_tarefa(args.task, args.workers, args.limit)
    else:
        interacao_terminal()

if __name__ == '__main__':
    main()