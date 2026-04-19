"""Camada de acesso a public.smt_usuario (CRUD + hashing bcrypt).

Niveis:
  0 = Admin            — tudo + gestao de usuarios
  1 = Operador         — opera os robos, relatorios, sincronizacao
  2 = Acompanhante     — visualiza execucoes (status/logs)
  3 = Visualizador     — apenas observatorio
"""
from __future__ import annotations

import os
import bcrypt

from database.connection import obter_conexao

NIVEIS_VALIDOS = {0, 1, 2, 3}


def _hash_senha(senha: str) -> str:
    return bcrypt.hashpw(senha.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verificar_senha(senha: str, hash_armazenado: str) -> bool:
    try:
        return bcrypt.checkpw(senha.encode("utf-8"), hash_armazenado.encode("utf-8"))
    except (ValueError, TypeError):
        return False


_DDL_SMT_USUARIO = """
CREATE TABLE IF NOT EXISTS public.smt_usuario (
    usr_id              SERIAL PRIMARY KEY,
    usr_username        VARCHAR(64) UNIQUE NOT NULL,
    usr_senha_hash      VARCHAR(255)       NOT NULL,
    usr_nivel           SMALLINT           NOT NULL DEFAULT 3
                         CHECK (usr_nivel BETWEEN 0 AND 3),
    usr_ativo           BOOLEAN            NOT NULL DEFAULT TRUE,
    usr_criado_em       TIMESTAMP          NOT NULL DEFAULT CURRENT_TIMESTAMP,
    usr_atualizado_em   TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_smt_usuario_ativo ON public.smt_usuario (usr_ativo);
"""


def garantir_admin_seed() -> None:
    """Garante a tabela smt_usuario e cria o admin a partir das variaveis de
    ambiente se ainda nao houver usuario. Executa no boot da API."""
    user = os.getenv("API_USER")
    pwd = os.getenv("API_PASS")
    conn = obter_conexao()
    try:
        cur = conn.cursor()
        cur.execute(_DDL_SMT_USUARIO)
        conn.commit()
        if not user or not pwd:
            cur.close()
            return
        cur.execute("SELECT COUNT(*) FROM public.smt_usuario")
        total = cur.fetchone()[0]
        if total > 0:
            cur.close()
            return
        cur.execute(
            """INSERT INTO public.smt_usuario (usr_username, usr_senha_hash, usr_nivel, usr_ativo)
                VALUES (%s, %s, 0, TRUE)""",
            (user, _hash_senha(pwd)),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def buscar_por_username(username: str):
    conn = obter_conexao()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT usr_id, usr_username, usr_senha_hash, usr_nivel, usr_ativo
               FROM public.smt_usuario WHERE usr_username = %s""",
            (username,),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return {
            "id": row[0],
            "username": row[1],
            "senha_hash": row[2],
            "nivel": row[3],
            "ativo": row[4],
        }
    finally:
        conn.close()


def buscar_por_id(usr_id: int):
    conn = obter_conexao()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT usr_id, usr_username, usr_nivel, usr_ativo
               FROM public.smt_usuario WHERE usr_id = %s""",
            (usr_id,),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return {"id": row[0], "username": row[1], "nivel": row[2], "ativo": row[3]}
    finally:
        conn.close()


def listar() -> list[dict]:
    conn = obter_conexao()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT usr_id, usr_username, usr_nivel, usr_ativo, usr_criado_em
               FROM public.smt_usuario ORDER BY usr_nivel, usr_username"""
        )
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "id": r[0],
                "username": r[1],
                "nivel": r[2],
                "ativo": r[3],
                "criado_em": r[4].isoformat() if r[4] else None,
            }
            for r in rows
        ]
    finally:
        conn.close()


def criar(username: str, senha: str, nivel: int) -> int:
    if nivel not in NIVEIS_VALIDOS:
        raise ValueError(f"nivel invalido: {nivel}")
    if not username or not senha:
        raise ValueError("username e senha sao obrigatorios")
    conn = obter_conexao()
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO public.smt_usuario (usr_username, usr_senha_hash, usr_nivel, usr_ativo)
               VALUES (%s, %s, %s, TRUE) RETURNING usr_id""",
            (username, _hash_senha(senha), nivel),
        )
        novo_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        return novo_id
    finally:
        conn.close()


def atualizar(usr_id: int, nivel: int | None = None, ativo: bool | None = None) -> None:
    campos = []
    valores: list = []
    if nivel is not None:
        if nivel not in NIVEIS_VALIDOS:
            raise ValueError(f"nivel invalido: {nivel}")
        campos.append("usr_nivel = %s")
        valores.append(nivel)
    if ativo is not None:
        campos.append("usr_ativo = %s")
        valores.append(bool(ativo))
    if not campos:
        return
    campos.append("usr_atualizado_em = CURRENT_TIMESTAMP")
    valores.append(usr_id)
    conn = obter_conexao()
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE public.smt_usuario SET {', '.join(campos)} WHERE usr_id = %s",
            tuple(valores),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def resetar_senha(usr_id: int, nova_senha: str) -> None:
    if not nova_senha or len(nova_senha) < 4:
        raise ValueError("senha deve ter pelo menos 4 caracteres")
    conn = obter_conexao()
    try:
        cur = conn.cursor()
        cur.execute(
            """UPDATE public.smt_usuario
               SET usr_senha_hash = %s, usr_atualizado_em = CURRENT_TIMESTAMP
               WHERE usr_id = %s""",
            (_hash_senha(nova_senha), usr_id),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def excluir(usr_id: int) -> None:
    conn = obter_conexao()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM public.smt_usuario WHERE usr_id = %s", (usr_id,))
        conn.commit()
        cur.close()
    finally:
        conn.close()


def contar_admins_ativos() -> int:
    conn = obter_conexao()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM public.smt_usuario WHERE usr_nivel = 0 AND usr_ativo = TRUE"
        )
        total = cur.fetchone()[0]
        cur.close()
        return int(total or 0)
    finally:
        conn.close()
