-- =====================================================================
-- 006: mv_media_municipio_fazendas
-- =====================================================================
-- Matview com mediana e media R$/ha por municipio, segmento fazendas
-- (anc_hectare >= 50), para tipologia nivel 0 (Geral) e nivel 1
-- (Agricola, Pecuaria, Floresta Plantada, Vegetacao Nativa).
--
-- Identificador externo: mun_cod (codigo IBGE). mun_id e usado apenas
-- internamente pelos joins.
--
-- Municipios sem anuncio no segmento recebem estatistica do vizinho
-- mais similar, priorizando (nessa ordem):
--   1. mesmo mercado regional
--   2. mesma UF
--   3. mesma regiao
--   4. qualquer municipio com dados
-- Criterio de similaridade dentro de cada nivel: menor distancia
-- euclidiana no vetor (apt_viaria, apt_hidro, apt_pedologica,
-- apt_geomorfologica), com desempate pela distancia geografica.
--
-- Coluna origem:
--   'propria'  -> estatistica calculada com anuncios do proprio municipio
--   'vizinho'  -> copiada do municipio_vizinho (mais similar com dados)
--   'sem_ref'  -> sem nenhum vizinho disponivel (NULLs nas metricas)

BEGIN;

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
stats_proprio AS (
    SELECT
        anc_mun_id AS mun_id,
        count(*)                                                   AS n_anuncios,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha)          AS mediana_geral,
        avg(vpha)                                                  AS media_geral,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha)
            FILTER (WHERE 1 = ANY(tips))                           AS mediana_agricola,
        avg(vpha) FILTER (WHERE 1 = ANY(tips))                     AS media_agricola,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha)
            FILTER (WHERE 2 = ANY(tips))                           AS mediana_pecuaria,
        avg(vpha) FILTER (WHERE 2 = ANY(tips))                     AS media_pecuaria,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha)
            FILTER (WHERE 3 = ANY(tips))                           AS mediana_floresta_plantada,
        avg(vpha) FILTER (WHERE 3 = ANY(tips))                     AS media_floresta_plantada,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha)
            FILTER (WHERE 4 = ANY(tips))                           AS mediana_vegetacao_nativa,
        avg(vpha) FILTER (WHERE 4 = ANY(tips))                     AS media_vegetacao_nativa
    FROM base_fazendas
    GROUP BY anc_mun_id
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
candidatos AS (
    -- Somente municipios que tem estatistica propria servem de referencia
    SELECT
        mu.mun_id,
        mu.mun_cod,
        mu.mun_unf_id,
        mu.mun_mre_id,
        mu.reg_id,
        mu.mun_centroid,
        mu.apt_viaria,
        mu.apt_hidro,
        mu.apt_pedologica,
        mu.apt_geomorfologica
    FROM munis mu
    JOIN stats_proprio sp ON sp.mun_id = mu.mun_id
),
vizinho_escolhido AS (
    -- Para cada municipio SEM dados, acha o candidato mais "parecido"
    SELECT
        sem.mun_id                   AS mun_id,
        viz.mun_id_vizinho,
        viz.mun_cod_vizinho,
        viz.dist_apt,
        viz.dist_km,
        viz.nivel_fallback
    FROM munis sem
    LEFT JOIN stats_proprio sp ON sp.mun_id = sem.mun_id
    CROSS JOIN LATERAL (
        SELECT
            c.mun_id  AS mun_id_vizinho,
            c.mun_cod AS mun_cod_vizinho,
            SQRT(
                POWER(c.apt_viaria        - sem.apt_viaria, 2) +
                POWER(c.apt_hidro         - sem.apt_hidro, 2) +
                POWER(c.apt_pedologica    - sem.apt_pedologica, 2) +
                POWER(c.apt_geomorfologica- sem.apt_geomorfologica, 2)
            )::double precision AS dist_apt,
            (ST_Distance(c.mun_centroid, sem.mun_centroid) / 1000.0) AS dist_km,
            CASE
                WHEN sem.mun_mre_id IS NOT NULL AND c.mun_mre_id = sem.mun_mre_id THEN 0
                WHEN c.mun_unf_id = sem.mun_unf_id                                 THEN 1
                WHEN c.reg_id     = sem.reg_id                                     THEN 2
                ELSE 3
            END AS nivel_fallback
        FROM candidatos c
        ORDER BY
            CASE
                WHEN sem.mun_mre_id IS NOT NULL AND c.mun_mre_id = sem.mun_mre_id THEN 0
                WHEN c.mun_unf_id = sem.mun_unf_id                                 THEN 1
                WHEN c.reg_id     = sem.reg_id                                     THEN 2
                ELSE 3
            END ASC,
            SQRT(
                POWER(c.apt_viaria        - sem.apt_viaria, 2) +
                POWER(c.apt_hidro         - sem.apt_hidro, 2) +
                POWER(c.apt_pedologica    - sem.apt_pedologica, 2) +
                POWER(c.apt_geomorfologica- sem.apt_geomorfologica, 2)
            ) ASC,
            ST_Distance(c.mun_centroid, sem.mun_centroid) ASC
        LIMIT 1
    ) viz
    WHERE sp.mun_id IS NULL  -- so precisa de vizinho quem nao tem dado proprio
)
SELECT
    m.mun_cod,
    m.regiao,
    m.mercado_regional,
    m.uf,
    m.municipio,
    COALESCE(s.n_anuncios, 0)                 AS n_anuncios,
    CASE
        WHEN s.mun_id IS NOT NULL             THEN 'propria'
        WHEN v.mun_id_vizinho IS NOT NULL     THEN 'vizinho'
        ELSE 'sem_ref'
    END                                       AS origem,
    v.mun_cod_vizinho,
    viz_mun.mun_nome                          AS municipio_vizinho,
    v.nivel_fallback,
    round(v.dist_km::numeric, 1)              AS dist_km_vizinho,
    round(v.dist_apt::numeric, 2)             AS dist_apt_vizinho,
    COALESCE(s.mediana_geral,             viz_s.mediana_geral)             AS mediana_geral,
    COALESCE(s.media_geral,               viz_s.media_geral)               AS media_geral,
    COALESCE(s.mediana_agricola,          viz_s.mediana_agricola)          AS mediana_agricola,
    COALESCE(s.media_agricola,            viz_s.media_agricola)            AS media_agricola,
    COALESCE(s.mediana_pecuaria,          viz_s.mediana_pecuaria)          AS mediana_pecuaria,
    COALESCE(s.media_pecuaria,            viz_s.media_pecuaria)            AS media_pecuaria,
    COALESCE(s.mediana_floresta_plantada, viz_s.mediana_floresta_plantada) AS mediana_floresta_plantada,
    COALESCE(s.media_floresta_plantada,   viz_s.media_floresta_plantada)   AS media_floresta_plantada,
    COALESCE(s.mediana_vegetacao_nativa,  viz_s.mediana_vegetacao_nativa)  AS mediana_vegetacao_nativa,
    COALESCE(s.media_vegetacao_nativa,    viz_s.media_vegetacao_nativa)    AS media_vegetacao_nativa
FROM munis m
LEFT JOIN stats_proprio s       ON s.mun_id       = m.mun_id
LEFT JOIN vizinho_escolhido v   ON v.mun_id       = m.mun_id
LEFT JOIN stats_proprio viz_s   ON viz_s.mun_id   = v.mun_id_vizinho
LEFT JOIN public.smt_municipio viz_mun ON viz_mun.mun_id = v.mun_id_vizinho
ORDER BY m.regiao, m.uf, m.municipio
WITH DATA;

-- Index UNIQUE exigido pelo REFRESH CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_media_municipio_fazendas_pk
    ON public.mv_media_municipio_fazendas (mun_cod);

CREATE INDEX IF NOT EXISTS ix_mv_media_mun_faz_origem
    ON public.mv_media_municipio_fazendas (origem);

CREATE INDEX IF NOT EXISTS ix_mv_media_mun_faz_uf
    ON public.mv_media_municipio_fazendas (uf);


-- Função de refresh que devolve resumo
CREATE OR REPLACE FUNCTION public.refresh_mv_media_municipio_fazendas()
RETURNS TABLE (
    total_municipios   bigint,
    com_dados_proprios bigint,
    com_fallback       bigint,
    sem_referencia     bigint,
    duracao            interval
)
LANGUAGE plpgsql AS $$
DECLARE
    t0 timestamptz := clock_timestamp();
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_media_municipio_fazendas;

    RETURN QUERY
    SELECT
        COUNT(*)::bigint,
        COUNT(*) FILTER (WHERE origem = 'propria')::bigint,
        COUNT(*) FILTER (WHERE origem = 'vizinho')::bigint,
        COUNT(*) FILTER (WHERE origem = 'sem_ref')::bigint,
        (clock_timestamp() - t0)::interval
    FROM public.mv_media_municipio_fazendas;
END;
$$;

COMMIT;
