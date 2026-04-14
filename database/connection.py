# =====================================================================
# ARQUIVO: database/connection.py
# DESCRIÇÃO: Gerenciador central de conexões PostGIS.
# =====================================================================

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def obter_conexao():
    """Retorna uma conexão ativa com o banco de dados do Supabase."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST"), 
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"), 
        password=os.getenv("DB_PASS"), 
        port=os.getenv("DB_PORT", "5432")
    )