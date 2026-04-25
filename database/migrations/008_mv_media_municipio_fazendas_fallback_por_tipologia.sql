-- =====================================================================
-- 008: mv_media_municipio_fazendas com fallback por tipologia
-- =====================================================================
-- Reescreve a matview criada em 006. Problema: municipios com 1-2
-- anuncios "soltos" (sem classificacao de tipologia nivel 1) ficavam
-- com origem='propria' mas mediana_agricola/pecuaria/etc. NULL --
-- porque o vizinho so era consultado quando o proprio nao tinha
-- nenhum dado. Resultado: 77-95% de NULLs nas colunas por tipologia.
--
-- Nova logica: fallback INDEPENDENTE por tipologia. Cada coluna
-- (geral / agricola / pecuaria / floresta_plantada / vegetacao_nativa)
-- pode vir do proprio municipio OU de um vizinho mais similar que
-- tenha aquela tipologia especifica.
--
-- As colunas de rastreabilidade do vizinho (mun_cod_vizinho etc.)
-- foram removidas porque com fallback por tipologia cada coluna
-- pode vir de um vizinho diferente -- expor tudo deixaria a tabela
-- larga demais. Como proxy de rastreabilidade ficam os contadores
-- n_anuncios / n_agricola / n_pecuaria / n_floresta_plantada /
-- n_vegetacao_nativa: valor 0 indica que a metrica correspondente
-- foi emprestada de vizinho.

BEGIN;

DROP VIEW IF EXISTS public.vw_media_municipio_fazendas_fmt;
DROP FUNCTION IF EXISTS public.refresh_mv_media_municipio_fazendas();
DROP MATERIALIZED VIEW IF EXISTS public.mv_media_municipio_fazendas;

CREATE MATERIALIZED VIEW public.mv_media_municipio_fazendas AS
WITH tip_por_anc AS (
    SELECT atp_anc_id, array_agg(atp_tip_id) AS tips
    FROM public.smt_anuncio_tipologia
    GROUP BY atp_anc_id
),
base_fazendas AS (
    SELECT
        a.anc_id,
        a.anc_mun_id,
        (a.anc_valor_total / a.anc_hectare)::double precision AS vpha,
        COALESCE(t.tips, ARRAY[]::integer[]) AS tips
    FROM public.smt_anuncio a
    LEFT JOIN tip_por_anc t ON t.atp_anc_id = a.anc_id
    WHERE a.anc_hectare >= 50
      AND a.anc_valor_total > 0
      AND (a.anc_valor_total / a.anc_hectare) BETWEEN 1000 AND 10000000
),
-- Uma CTE de estatistica por tipologia. Cada uma so inclui
-- municipios que TEM pelo menos um anuncio da respectiva tipologia.
s_geral AS (
    SELECT anc_mun_id AS mun_id,
           count(*) AS n,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha) AS mediana,
           avg(vpha) AS media
    FROM base_fazendas GROUP BY anc_mun_id
),
s_agricola AS (
    SELECT anc_mun_id AS mun_id, count(*) AS n,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha) AS mediana,
           avg(vpha) AS media
    FROM base_fazendas WHERE 1 = ANY(tips) GROUP BY anc_mun_id
),
s_pecuaria AS (
    SELECT anc_mun_id AS mun_id, count(*) AS n,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha) AS mediana,
           avg(vpha) AS media
    FROM base_fazendas WHERE 2 = ANY(tips) GROUP BY anc_mun_id
),
s_fp AS (
    SELECT anc_mun_id AS mun_id, count(*) AS n,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha) AS mediana,
           avg(vpha) AS media
    FROM base_fazendas WHERE 3 = ANY(tips) GROUP BY anc_mun_id
),
s_vn AS (
    SELECT anc_mun_id AS mun_id, count(*) AS n,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha) AS mediana,
           avg(vpha) AS media
    FROM base_fazendas WHERE 4 = ANY(tips) GROUP BY anc_mun_id
),
munis AS (
    SELECT
        m.mun_id,
        m.mun_cod,
        m.mun_nome            AS municipio,
        uf.unf_sigla          AS uf,
        uf.unf_reg_id         AS reg_id,
        CASE uf.unf_reg_id
            WHEN 1 THEN 'Norte'
            WHEN 2 THEN 'Nordeste'
            WHEN 3 THEN 'Sudeste'
            WHEN 4 THEN 'Sul'
            WHEN 5 THEN 'Centro-Oeste'
            ELSE 'Desconhecida'
        END                   AS regiao,
        mr.mre_nome           AS mercado_regional,
        m.mun_unf_id,
        m.mun_mre_id,
        ST_Centroid(m.mun_geom)::geography AS mun_centroid,
        COALESCE(apt.apt_viaria, 3)        AS apt_viaria,
        COALESCE(apt.apt_hidro, 3)         AS apt_hidro,
        COALESCE(apt.apt_pedologica, 3)    AS apt_pedologica,
        COALESCE(apt.apt_geomorfologica, 3) AS apt_geomorfologica
    FROM public.smt_municipio m
    JOIN public.smt_unidade_federativa uf ON m.mun_unf_id = uf.unf_id
    LEFT JOIN public.smt_mercado_regional mr ON m.mun_mre_id = mr.mre_id
    LEFT JOIN public.smt_municipio_aptidao apt ON apt.apt_mun_id = m.mun_id
),
-- Candidatos por tipologia: pares (municipio, metricas) com
-- chaves de aptidao e localizacao, pra usar em LATERAL JOIN.
-- Filtramos por tipologia incluindo APENAS candidatos que tem
-- essa estatistica (evita o LATERAL escolher vizinho com NULL).
cand AS (
    SELECT
        mu.mun_id,
        mu.mun_cod,
        mu.mun_unf_id,
        mu.mun_mre_id,
        mu.reg_id,
        mu.mun_centroid,
        mu.apt_viaria, mu.apt_hidro, mu.apt_pedologica, mu.apt_geomorfologica
    FROM munis mu
)
SELECT
    m.mun_cod,
    m.regiao,
    m.mercado_regional,
    m.uf,
    m.municipio,
    COALESCE(s_g.n, 0)   AS n_anuncios,
    COALESCE(s_a.n, 0)   AS n_agricola,
    COALESCE(s_p.n, 0)   AS n_pecuaria,
    COALESCE(s_f.n, 0)   AS n_floresta_plantada,
    COALESCE(s_v.n, 0)   AS n_vegetacao_nativa,

    -- GERAL
    COALESCE(s_g.mediana, viz_g.mediana) AS mediana_geral,
    COALESCE(s_g.media,   viz_g.media)   AS media_geral,
    -- AGRICOLA
    COALESCE(s_a.mediana, viz_a.mediana) AS mediana_agricola,
    COALESCE(s_a.media,   viz_a.media)   AS media_agricola,
    -- PECUARIA
    COALESCE(s_p.mediana, viz_p.mediana) AS mediana_pecuaria,
    COALESCE(s_p.media,   viz_p.media)   AS media_pecuaria,
    -- FLORESTA PLANTADA
    COALESCE(s_f.mediana, viz_f.mediana) AS mediana_floresta_plantada,
    COALESCE(s_f.media,   viz_f.media)   AS media_floresta_plantada,
    -- VEGETACAO NATIVA
    COALESCE(s_v.mediana, viz_v.mediana) AS mediana_vegetacao_nativa,
    COALESCE(s_v.media,   viz_v.media)   AS media_vegetacao_nativa
FROM munis m

LEFT JOIN s_geral    s_g ON s_g.mun_id = m.mun_id
LEFT JOIN s_agricola s_a ON s_a.mun_id = m.mun_id
LEFT JOIN s_pecuaria s_p ON s_p.mun_id = m.mun_id
LEFT JOIN s_fp       s_f ON s_f.mun_id = m.mun_id
LEFT JOIN s_vn       s_v ON s_v.mun_id = m.mun_id

-- LATERAL vizinho pra GERAL (candidatos = quem tem s_geral)
LEFT JOIN LATERAL (
    SELECT sg.mediana, sg.media
    FROM cand c JOIN s_geral sg ON sg.mun_id = c.mun_id
    ORDER BY
        CASE
            WHEN m.mun_mre_id IS NOT NULL AND c.mun_mre_id = m.mun_mre_id THEN 0
            WHEN c.mun_unf_id = m.mun_unf_id                              THEN 1
            WHEN c.reg_id     = m.reg_id                                  THEN 2
            ELSE 3
        END,
        POWER(c.apt_viaria - m.apt_viaria, 2) + POWER(c.apt_hidro - m.apt_hidro, 2) +
        POWER(c.apt_pedologica - m.apt_pedologica, 2) + POWER(c.apt_geomorfologica - m.apt_geomorfologica, 2),
        ST_Distance(c.mun_centroid, m.mun_centroid)
    LIMIT 1
) viz_g ON s_g.n IS NULL

-- LATERAL vizinho pra AGRICOLA
LEFT JOIN LATERAL (
    SELECT sa.mediana, sa.media
    FROM cand c JOIN s_agricola sa ON sa.mun_id = c.mun_id
    ORDER BY
        CASE
            WHEN m.mun_mre_id IS NOT NULL AND c.mun_mre_id = m.mun_mre_id THEN 0
            WHEN c.mun_unf_id = m.mun_unf_id                              THEN 1
            WHEN c.reg_id     = m.reg_id                                  THEN 2
            ELSE 3
        END,
        POWER(c.apt_viaria - m.apt_viaria, 2) + POWER(c.apt_hidro - m.apt_hidro, 2) +
        POWER(c.apt_pedologica - m.apt_pedologica, 2) + POWER(c.apt_geomorfologica - m.apt_geomorfologica, 2),
        ST_Distance(c.mun_centroid, m.mun_centroid)
    LIMIT 1
) viz_a ON s_a.n IS NULL

-- LATERAL vizinho pra PECUARIA
LEFT JOIN LATERAL (
    SELECT sp.mediana, sp.media
    FROM cand c JOIN s_pecuaria sp ON sp.mun_id = c.mun_id
    ORDER BY
        CASE
            WHEN m.mun_mre_id IS NOT NULL AND c.mun_mre_id = m.mun_mre_id THEN 0
            WHEN c.mun_unf_id = m.mun_unf_id                              THEN 1
            WHEN c.reg_id     = m.reg_id                                  THEN 2
            ELSE 3
        END,
        POWER(c.apt_viaria - m.apt_viaria, 2) + POWER(c.apt_hidro - m.apt_hidro, 2) +
        POWER(c.apt_pedologica - m.apt_pedologica, 2) + POWER(c.apt_geomorfologica - m.apt_geomorfologica, 2),
        ST_Distance(c.mun_centroid, m.mun_centroid)
    LIMIT 1
) viz_p ON s_p.n IS NULL

-- LATERAL vizinho pra FLORESTA PLANTADA
LEFT JOIN LATERAL (
    SELECT sf.mediana, sf.media
    FROM cand c JOIN s_fp sf ON sf.mun_id = c.mun_id
    ORDER BY
        CASE
            WHEN m.mun_mre_id IS NOT NULL AND c.mun_mre_id = m.mun_mre_id THEN 0
            WHEN c.mun_unf_id = m.mun_unf_id                              THEN 1
            WHEN c.reg_id     = m.reg_id                                  THEN 2
            ELSE 3
        END,
        POWER(c.apt_viaria - m.apt_viaria, 2) + POWER(c.apt_hidro - m.apt_hidro, 2) +
        POWER(c.apt_pedologica - m.apt_pedologica, 2) + POWER(c.apt_geomorfologica - m.apt_geomorfologica, 2),
        ST_Distance(c.mun_centroid, m.mun_centroid)
    LIMIT 1
) viz_f ON s_f.n IS NULL

-- LATERAL vizinho pra VEGETACAO NATIVA
LEFT JOIN LATERAL (
    SELECT sv.mediana, sv.media
    FROM cand c JOIN s_vn sv ON sv.mun_id = c.mun_id
    ORDER BY
        CASE
            WHEN m.mun_mre_id IS NOT NULL AND c.mun_mre_id = m.mun_mre_id THEN 0
            WHEN c.mun_unf_id = m.mun_unf_id                              THEN 1
            WHEN c.reg_id     = m.reg_id                                  THEN 2
            ELSE 3
        END,
        POWER(c.apt_viaria - m.apt_viaria, 2) + POWER(c.apt_hidro - m.apt_hidro, 2) +
        POWER(c.apt_pedologica - m.apt_pedologica, 2) + POWER(c.apt_geomorfologica - m.apt_geomorfologica, 2),
        ST_Distance(c.mun_centroid, m.mun_centroid)
    LIMIT 1
) viz_v ON s_v.n IS NULL

ORDER BY m.regiao, m.uf, m.municipio
WITH DATA;

-- Indices
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_media_municipio_fazendas_pk
    ON public.mv_media_municipio_fazendas (mun_cod);

CREATE INDEX IF NOT EXISTS ix_mv_media_mun_faz_uf
    ON public.mv_media_municipio_fazendas (uf);


-- Função de refresh
CREATE OR REPLACE FUNCTION public.refresh_mv_media_municipio_fazendas()
RETURNS TABLE (
    total_municipios bigint,
    com_anuncios     bigint,
    sem_anuncios     bigint,
    duracao          interval
)
LANGUAGE plpgsql AS $$
DECLARE
    t0 timestamptz := clock_timestamp();
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_media_municipio_fazendas;

    RETURN QUERY
    SELECT
        COUNT(*)::bigint,
        COUNT(*) FILTER (WHERE n_anuncios > 0)::bigint,
        COUNT(*) FILTER (WHERE n_anuncios = 0)::bigint,
        (clock_timestamp() - t0)::interval
    FROM public.mv_media_municipio_fazendas;
END;
$$;


-- Recria a view formatada (BR), agora sem as colunas de rastreabilidade
CREATE VIEW public.vw_media_municipio_fazendas_fmt AS
SELECT
    mun_cod,
    regiao,
    mercado_regional,
    uf,
    municipio,
    n_anuncios,
    n_agricola,
    n_pecuaria,
    n_floresta_plantada,
    n_vegetacao_nativa,
    CASE WHEN mediana_geral             IS NULL THEN NULL ELSE translate(to_char(mediana_geral::numeric,             'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS mediana_geral,
    CASE WHEN media_geral               IS NULL THEN NULL ELSE translate(to_char(media_geral::numeric,               'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS media_geral,
    CASE WHEN mediana_agricola          IS NULL THEN NULL ELSE translate(to_char(mediana_agricola::numeric,          'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS mediana_agricola,
    CASE WHEN media_agricola            IS NULL THEN NULL ELSE translate(to_char(media_agricola::numeric,            'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS media_agricola,
    CASE WHEN mediana_pecuaria          IS NULL THEN NULL ELSE translate(to_char(mediana_pecuaria::numeric,          'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS mediana_pecuaria,
    CASE WHEN media_pecuaria            IS NULL THEN NULL ELSE translate(to_char(media_pecuaria::numeric,            'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS media_pecuaria,
    CASE WHEN mediana_floresta_plantada IS NULL THEN NULL ELSE translate(to_char(mediana_floresta_plantada::numeric, 'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS mediana_floresta_plantada,
    CASE WHEN media_floresta_plantada   IS NULL THEN NULL ELSE translate(to_char(media_floresta_plantada::numeric,   'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS media_floresta_plantada,
    CASE WHEN mediana_vegetacao_nativa  IS NULL THEN NULL ELSE translate(to_char(mediana_vegetacao_nativa::numeric,  'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS mediana_vegetacao_nativa,
    CASE WHEN media_vegetacao_nativa    IS NULL THEN NULL ELSE translate(to_char(media_vegetacao_nativa::numeric,    'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS media_vegetacao_nativa
FROM public.mv_media_municipio_fazendas;

COMMIT;
