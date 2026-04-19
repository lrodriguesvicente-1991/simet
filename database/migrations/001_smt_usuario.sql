-- =====================================================================
-- Migration 001: Tabela de usuarios do SIMET
-- Niveis: 0=Admin, 1=Operador, 2=Acompanhante, 3=Visualizador
-- =====================================================================

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
