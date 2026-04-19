-- =====================================================================
-- 002: Adiciona media por tipologia na matview mv_estatisticas_simet
-- =====================================================================
-- Motivo: o frontend passa a ter toggle "Media x Mediana" nas KPIs
-- e na tabela detalhada, exibindo as 4 tipologias de nivel 1
-- (agricola, pecuaria, floresta plantada, floresta nativa).
--
-- Mantidos sem alteracao:
--   - bins de categoria_tamanho (5 ha / 50 ha)
--   - ausencia de filtro de outlier
--   - todas as colunas existentes
-- (Mudancas metodologicas -- trim de outlier, bins novos -- ficam para
-- migracao separada, com aprovacao explicita.)

BEGIN;

DROP VIEW IF EXISTS public.vw_media_mercado_terras;
DROP MATERIALIZED VIEW IF EXISTS public.mv_estatisticas_simet;

CREATE MATERIALIZED VIEW public.mv_estatisticas_simet AS
WITH tip_agg AS (
    SELECT atp_anc_id, array_agg(atp_tip_id) AS tips
    FROM public.smt_anuncio_tipologia
    GROUP BY atp_anc_id
),
base AS (
    SELECT
        a.anc_id,
        a.anc_mun_id,
        (a.anc_valor_total / a.anc_hectare)::double precision AS vpha,
        CASE
            WHEN a.anc_hectare < 5 THEN 'Chacaras (< 5 ha)'
            WHEN a.anc_hectare <= 50 THEN 'Sitios (5 a 50 ha)'
            ELSE 'Fazendas (> 50 ha)'
        END AS categoria_tamanho,
        COALESCE(t.tips, ARRAY[]::integer[]) AS tips
    FROM public.smt_anuncio a
    LEFT JOIN tip_agg t ON t.atp_anc_id = a.anc_id
    WHERE a.anc_hectare > 0 AND a.anc_valor_total > 0
)
SELECT
    anc_mun_id,
    categoria_tamanho,
    count(*) AS total_anuncios_reais,

    -- medianas
    percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha) AS mediana_geral,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha)
        FILTER (WHERE 1 = ANY(tips)) AS mediana_agricola,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha)
        FILTER (WHERE 2 = ANY(tips)) AS mediana_pecuaria,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha)
        FILTER (WHERE 3 = ANY(tips)) AS mediana_floresta_plantada,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha)
        FILTER (WHERE 4 = ANY(tips)) AS mediana_floresta_nativa,

    -- contagens por tipologia
    count(*) FILTER (WHERE 1 = ANY(tips)) AS n_agricola,
    count(*) FILTER (WHERE 2 = ANY(tips)) AS n_pecuaria,
    count(*) FILTER (WHERE 3 = ANY(tips)) AS n_floresta_plantada,
    count(*) FILTER (WHERE 4 = ANY(tips)) AS n_floresta_nativa,

    -- medias (NOVAS colunas por tipologia)
    round(avg(vpha)::numeric, 2) AS media_geral,
    round(avg(vpha) FILTER (WHERE 1 = ANY(tips))::numeric, 2) AS media_agricola,
    round(avg(vpha) FILTER (WHERE 2 = ANY(tips))::numeric, 2) AS media_pecuaria,
    round(avg(vpha) FILTER (WHERE 3 = ANY(tips))::numeric, 2) AS media_floresta_plantada,
    round(avg(vpha) FILTER (WHERE 4 = ANY(tips))::numeric, 2) AS media_floresta_nativa,

    round(stddev(vpha)::numeric, 2) AS desvio_padrao,
    round(((stddev(vpha) / NULLIF(avg(vpha), 0::double precision)) * 100::double precision)::numeric, 2)
        AS coef_dispersao_pct
FROM base
GROUP BY anc_mun_id, categoria_tamanho
WITH DATA;

-- Indice UNIQUE e prerequisito para REFRESH CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_estatisticas_simet_pk
    ON public.mv_estatisticas_simet (anc_mun_id, categoria_tamanho);

-- Recria a view de consumo, agora com as medias por tipologia
CREATE VIEW public.vw_media_mercado_terras AS
SELECT
    m.mun_nome AS municipio,
    uf.unf_sigla AS estado,
    CASE uf.unf_reg_id
        WHEN 1 THEN 'Norte'::text
        WHEN 2 THEN 'Nordeste'::text
        WHEN 3 THEN 'Sudeste'::text
        WHEN 4 THEN 'Sul'::text
        WHEN 5 THEN 'Centro-Oeste'::text
        ELSE 'Desconhecida'::text
    END AS regiao,
    calc.categoria_tamanho,
    calc.total_anuncios_reais,
    calc.mediana_geral,
    calc.mediana_agricola,
    calc.mediana_pecuaria,
    calc.mediana_floresta_plantada,
    calc.mediana_floresta_nativa,
    calc.n_agricola,
    calc.n_pecuaria,
    calc.n_floresta_plantada,
    calc.n_floresta_nativa,
    calc.media_geral,
    calc.media_agricola,
    calc.media_pecuaria,
    calc.media_floresta_plantada,
    calc.media_floresta_nativa,
    calc.desvio_padrao,
    calc.coef_dispersao_pct,
    malha_mun.mlm_geom AS geom_municipio,
    malha_uf.mlu_geom  AS geom_estado,
    malha_reg.mlr_geom AS geom_regiao
FROM public.mv_estatisticas_simet calc
JOIN public.smt_municipio m ON calc.anc_mun_id = m.mun_id
JOIN public.smt_unidade_federativa uf ON m.mun_unf_id = uf.unf_id
LEFT JOIN public.smt_malha_municipal malha_mun ON m.mun_cod = malha_mun.mlm_cd_mun::integer
LEFT JOIN public.smt_malha_uf malha_uf ON uf.unf_cod = malha_uf.mlu_cd_uf::integer
LEFT JOIN public.smt_malha_regiao malha_reg ON uf.unf_reg_id = malha_reg.mlr_cd_regiao::integer;

COMMIT;
