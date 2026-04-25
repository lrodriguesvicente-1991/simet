-- =====================================================================
-- 005: Status PENDENTE_IA e REJEITADO_IA em smt_fila_processamento
-- =====================================================================
-- Motivo: separar "erro transitorio / parser" (PENDENTE regular) de
-- "erro estrutural que so a IA tenta resolver" (PENDENTE_IA) e de
-- "IA tambem falhou, so intervencao humana" (REJEITADO_IA).
--
-- A coluna fil_status nao tem CHECK constraint, entao nao precisa
-- alterar schema -- novos valores sao aceitos. Esta migration apenas
-- cria indices parciais para manter performance das queries das filas
-- e documenta os valores permitidos.
--
-- Valores esperados em fil_status:
--   PENDENTE       - fila normal, extrator determinstico ou IA pega
--   PENDENTE_IA    - so extrator em modo IA pega (ECI com GPU)
--   PROCESSANDO    - worker travou este registro
--   CONCLUIDO      - salvo em smt_anuncio
--   ERRO           - falhou, ACI audita e reclassifica
--   REJEITADO_IA   - IA tambem falhou, so humano resolve

BEGIN;

-- Indice parcial para o ECI puxar rapido as pendentes (normais e IA).
-- Acelera o ORDER BY fil_id DESC LIMIT 1 da query de captura.
CREATE INDEX IF NOT EXISTS ix_smt_fila_pendente_id_desc
    ON public.smt_fila_processamento (fil_id DESC)
    WHERE fil_status = 'PENDENTE';

CREATE INDEX IF NOT EXISTS ix_smt_fila_pendente_ia_id_desc
    ON public.smt_fila_processamento (fil_id DESC)
    WHERE fil_status = 'PENDENTE_IA';

-- Indice para o ACI filtrar rapido os ERRO.
CREATE INDEX IF NOT EXISTS ix_smt_fila_erro
    ON public.smt_fila_processamento (fil_id)
    WHERE fil_status = 'ERRO';

COMMIT;
