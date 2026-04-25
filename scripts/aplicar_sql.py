"""Aplica um arquivo .sql no banco do SIMET usando o .env do projeto.

Uso:
    python scripts/aplicar_sql.py database/migrations/011_mv_centroide_municipio.sql

Aceita varios arquivos de uma vez:
    python scripts/aplicar_sql.py database/migrations/011_*.sql database/migrations/012_*.sql

O script abre uma conexao por arquivo, executa em transacao unica e imprime
tempo + linhas afetadas. Em caso de erro, faz rollback e sai com codigo 1.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

# Garante que o diretorio raiz do projeto esta no sys.path
RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from database.connection import obter_conexao  # noqa: E402


def aplicar(arquivo: Path) -> bool:
    if not arquivo.exists():
        print(f"  [X] arquivo nao encontrado: {arquivo}", flush=True)
        return False

    sql = arquivo.read_text(encoding="utf-8")
    if not sql.strip():
        print(f"  [.] vazio, pulando: {arquivo.name}", flush=True)
        return True

    print(f">> aplicando {arquivo.name} ...", flush=True)
    t0 = time.time()
    conn = obter_conexao()
    try:
        # O psycopg2 abre transacao implicita; o BEGIN/COMMIT no proprio .sql
        # e tolerado (sao no-ops dentro de transacao ja aberta).
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        print(f"  [OK] ok em {time.time() - t0:.2f}s", flush=True)
        return True
    except Exception as e:
        conn.rollback()
        print(f"  [ERR] erro apos {time.time() - t0:.2f}s: {e}", flush=True)
        return False
    finally:
        conn.close()


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return 2

    falhas = 0
    for arg in args:
        if not aplicar(Path(arg)):
            falhas += 1

    if falhas:
        print(f"\n{falhas} arquivo(s) com erro.", flush=True)
        return 1
    print("\nTudo aplicado com sucesso.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
