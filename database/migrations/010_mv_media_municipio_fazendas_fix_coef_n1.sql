-- =====================================================================
-- 010: Corrige NULLs de coef_dispersao quando o municipio tem 1 anuncio
-- =====================================================================
-- A migration 009 usava `ON s_X.n IS NULL` como gatilho do LATERAL de
-- vizinhanca. Com isso, municipios que tem exatamente 1 anuncio da
-- tipologia ficavam com mediana/media proprias (ok) mas coef_dispersao
-- NULL -- porque stddev() (stddev_samp) precisa de >= 2 observacoes.
--
-- Correcao: condicional passa a ser `ON s_X.coef_disp IS NULL`. Cobre
-- os dois casos (sem anuncio proprio OU com 1 so). Mediana/media
-- continuam saindo do proprio via COALESCE quando existem.

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
s_geral AS (
    SELECT anc_mun_id AS mun_id, count(*) AS n,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha) AS mediana,
           avg(vpha) AS media,
           (stddev(vpha) / NULLIF(avg(vpha), 0) * 100) AS coef_disp
    FROM base_fazendas GROUP BY anc_mun_id
),
s_agricola AS (
    SELECT anc_mun_id AS mun_id, count(*) AS n,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha) AS mediana,
           avg(vpha) AS media,
           (stddev(vpha) / NULLIF(avg(vpha), 0) * 100) AS coef_disp
    FROM base_fazendas WHERE 1 = ANY(tips) GROUP BY anc_mun_id
),
s_pecuaria AS (
    SELECT anc_mun_id AS mun_id, count(*) AS n,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha) AS mediana,
           avg(vpha) AS media,
           (stddev(vpha) / NULLIF(avg(vpha), 0) * 100) AS coef_disp
    FROM base_fazendas WHERE 2 = ANY(tips) GROUP BY anc_mun_id
),
s_fp AS (
    SELECT anc_mun_id AS mun_id, count(*) AS n,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha) AS mediana,
           avg(vpha) AS media,
           (stddev(vpha) / NULLIF(avg(vpha), 0) * 100) AS coef_disp
    FROM base_fazendas WHERE 3 = ANY(tips) GROUP BY anc_mun_id
),
s_vn AS (
    SELECT anc_mun_id AS mun_id, count(*) AS n,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY vpha) AS mediana,
           avg(vpha) AS media,
           (stddev(vpha) / NULLIF(avg(vpha), 0) * 100) AS coef_disp
    FROM base_fazendas WHERE 4 = ANY(tips) GROUP BY anc_mun_id
),
munis AS (
    SELECT
        m.mun_id, m.mun_cod, m.mun_nome AS municipio,
        uf.unf_sigla AS uf, uf.unf_reg_id AS reg_id,
        CASE uf.unf_reg_id
            WHEN 1 THEN 'Norte' WHEN 2 THEN 'Nordeste'
            WHEN 3 THEN 'Sudeste' WHEN 4 THEN 'Sul'
            WHEN 5 THEN 'Centro-Oeste' ELSE 'Desconhecida'
        END AS regiao,
        mr.mre_nome AS mercado_regional,
        m.mun_unf_id, m.mun_mre_id,
        ST_Centroid(m.mun_geom)::geography AS mun_centroid,
        COALESCE(apt.apt_viaria, 3) AS apt_viaria,
        COALESCE(apt.apt_hidro, 3) AS apt_hidro,
        COALESCE(apt.apt_pedologica, 3) AS apt_pedologica,
        COALESCE(apt.apt_geomorfologica, 3) AS apt_geomorfologica
    FROM public.smt_municipio m
    JOIN public.smt_unidade_federativa uf ON m.mun_unf_id = uf.unf_id
    LEFT JOIN public.smt_mercado_regional mr ON m.mun_mre_id = mr.mre_id
    LEFT JOIN public.smt_municipio_aptidao apt ON apt.apt_mun_id = m.mun_id
),
cand AS (
    SELECT mu.mun_id, mu.mun_cod, mu.mun_unf_id, mu.mun_mre_id, mu.reg_id,
           mu.mun_centroid,
           mu.apt_viaria, mu.apt_hidro, mu.apt_pedologica, mu.apt_geomorfologica
    FROM munis mu
)
SELECT
    m.mun_cod, m.regiao, m.mercado_regional, m.uf, m.municipio,
    COALESCE(s_g.n, 0) AS n_anuncios,
    COALESCE(s_a.n, 0) AS n_agricola,
    COALESCE(s_p.n, 0) AS n_pecuaria,
    COALESCE(s_f.n, 0) AS n_floresta_plantada,
    COALESCE(s_v.n, 0) AS n_vegetacao_nativa,

    COALESCE(s_g.mediana, viz_g.mediana) AS mediana_geral,
    COALESCE(s_g.media,   viz_g.media)   AS media_geral,
    COALESCE(s_g.coef_disp, viz_g.coef_disp) AS coef_dispersao_geral,

    COALESCE(s_a.mediana, viz_a.mediana) AS mediana_agricola,
    COALESCE(s_a.media,   viz_a.media)   AS media_agricola,
    COALESCE(s_a.coef_disp, viz_a.coef_disp) AS coef_dispersao_agricola,

    COALESCE(s_p.mediana, viz_p.mediana) AS mediana_pecuaria,
    COALESCE(s_p.media,   viz_p.media)   AS media_pecuaria,
    COALESCE(s_p.coef_disp, viz_p.coef_disp) AS coef_dispersao_pecuaria,

    COALESCE(s_f.mediana, viz_f.mediana) AS mediana_floresta_plantada,
    COALESCE(s_f.media,   viz_f.media)   AS media_floresta_plantada,
    COALESCE(s_f.coef_disp, viz_f.coef_disp) AS coef_dispersao_floresta_plantada,

    COALESCE(s_v.mediana, viz_v.mediana) AS mediana_vegetacao_nativa,
    COALESCE(s_v.media,   viz_v.media)   AS media_vegetacao_nativa,
    COALESCE(s_v.coef_disp, viz_v.coef_disp) AS coef_dispersao_vegetacao_nativa

FROM munis m

LEFT JOIN s_geral    s_g ON s_g.mun_id = m.mun_id
LEFT JOIN s_agricola s_a ON s_a.mun_id = m.mun_id
LEFT JOIN s_pecuaria s_p ON s_p.mun_id = m.mun_id
LEFT JOIN s_fp       s_f ON s_f.mun_id = m.mun_id
LEFT JOIN s_vn       s_v ON s_v.mun_id = m.mun_id

-- LATERALs: disparam quando coef_disp e NULL. Cobre n=0 (tudo NULL no proprio)
-- e n=1 (mediana/media proprias, mas stddev NULL).
-- O filtro WHERE sg.coef_disp IS NOT NULL dentro do LATERAL garante
-- que so entram como vizinhos os municipios que tem coef proprio.
LEFT JOIN LATERAL (
    SELECT sg.mediana, sg.media, sg.coef_disp
    FROM cand c JOIN s_geral sg ON sg.mun_id = c.mun_id
    WHERE sg.coef_disp IS NOT NULL
    ORDER BY
        CASE WHEN m.mun_mre_id IS NOT NULL AND c.mun_mre_id = m.mun_mre_id THEN 0
             WHEN c.mun_unf_id = m.mun_unf_id THEN 1
             WHEN c.reg_id = m.reg_id THEN 2 ELSE 3 END,
        POWER(c.apt_viaria - m.apt_viaria, 2) + POWER(c.apt_hidro - m.apt_hidro, 2) +
        POWER(c.apt_pedologica - m.apt_pedologica, 2) + POWER(c.apt_geomorfologica - m.apt_geomorfologica, 2),
        ST_Distance(c.mun_centroid, m.mun_centroid)
    LIMIT 1
) viz_g ON s_g.coef_disp IS NULL

LEFT JOIN LATERAL (
    SELECT sa.mediana, sa.media, sa.coef_disp
    FROM cand c JOIN s_agricola sa ON sa.mun_id = c.mun_id
    WHERE sa.coef_disp IS NOT NULL
    ORDER BY
        CASE WHEN m.mun_mre_id IS NOT NULL AND c.mun_mre_id = m.mun_mre_id THEN 0
             WHEN c.mun_unf_id = m.mun_unf_id THEN 1
             WHEN c.reg_id = m.reg_id THEN 2 ELSE 3 END,
        POWER(c.apt_viaria - m.apt_viaria, 2) + POWER(c.apt_hidro - m.apt_hidro, 2) +
        POWER(c.apt_pedologica - m.apt_pedologica, 2) + POWER(c.apt_geomorfologica - m.apt_geomorfologica, 2),
        ST_Distance(c.mun_centroid, m.mun_centroid)
    LIMIT 1
) viz_a ON s_a.coef_disp IS NULL

LEFT JOIN LATERAL (
    SELECT sp.mediana, sp.media, sp.coef_disp
    FROM cand c JOIN s_pecuaria sp ON sp.mun_id = c.mun_id
    WHERE sp.coef_disp IS NOT NULL
    ORDER BY
        CASE WHEN m.mun_mre_id IS NOT NULL AND c.mun_mre_id = m.mun_mre_id THEN 0
             WHEN c.mun_unf_id = m.mun_unf_id THEN 1
             WHEN c.reg_id = m.reg_id THEN 2 ELSE 3 END,
        POWER(c.apt_viaria - m.apt_viaria, 2) + POWER(c.apt_hidro - m.apt_hidro, 2) +
        POWER(c.apt_pedologica - m.apt_pedologica, 2) + POWER(c.apt_geomorfologica - m.apt_geomorfologica, 2),
        ST_Distance(c.mun_centroid, m.mun_centroid)
    LIMIT 1
) viz_p ON s_p.coef_disp IS NULL

LEFT JOIN LATERAL (
    SELECT sf.mediana, sf.media, sf.coef_disp
    FROM cand c JOIN s_fp sf ON sf.mun_id = c.mun_id
    WHERE sf.coef_disp IS NOT NULL
    ORDER BY
        CASE WHEN m.mun_mre_id IS NOT NULL AND c.mun_mre_id = m.mun_mre_id THEN 0
             WHEN c.mun_unf_id = m.mun_unf_id THEN 1
             WHEN c.reg_id = m.reg_id THEN 2 ELSE 3 END,
        POWER(c.apt_viaria - m.apt_viaria, 2) + POWER(c.apt_hidro - m.apt_hidro, 2) +
        POWER(c.apt_pedologica - m.apt_pedologica, 2) + POWER(c.apt_geomorfologica - m.apt_geomorfologica, 2),
        ST_Distance(c.mun_centroid, m.mun_centroid)
    LIMIT 1
) viz_f ON s_f.coef_disp IS NULL

LEFT JOIN LATERAL (
    SELECT sv.mediana, sv.media, sv.coef_disp
    FROM cand c JOIN s_vn sv ON sv.mun_id = c.mun_id
    WHERE sv.coef_disp IS NOT NULL
    ORDER BY
        CASE WHEN m.mun_mre_id IS NOT NULL AND c.mun_mre_id = m.mun_mre_id THEN 0
             WHEN c.mun_unf_id = m.mun_unf_id THEN 1
             WHEN c.reg_id = m.reg_id THEN 2 ELSE 3 END,
        POWER(c.apt_viaria - m.apt_viaria, 2) + POWER(c.apt_hidro - m.apt_hidro, 2) +
        POWER(c.apt_pedologica - m.apt_pedologica, 2) + POWER(c.apt_geomorfologica - m.apt_geomorfologica, 2),
        ST_Distance(c.mun_centroid, m.mun_centroid)
    LIMIT 1
) viz_v ON s_v.coef_disp IS NULL

ORDER BY m.regiao, m.uf, m.municipio
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_media_municipio_fazendas_pk
    ON public.mv_media_municipio_fazendas (mun_cod);
CREATE INDEX IF NOT EXISTS ix_mv_media_mun_faz_uf
    ON public.mv_media_municipio_fazendas (uf);


CREATE OR REPLACE FUNCTION public.refresh_mv_media_municipio_fazendas()
RETURNS TABLE (
    total_municipios bigint,
    com_anuncios     bigint,
    sem_anuncios     bigint,
    duracao          interval
)
LANGUAGE plpgsql AS $$
DECLARE t0 timestamptz := clock_timestamp();
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_media_municipio_fazendas;
    RETURN QUERY
    SELECT COUNT(*)::bigint,
           COUNT(*) FILTER (WHERE n_anuncios > 0)::bigint,
           COUNT(*) FILTER (WHERE n_anuncios = 0)::bigint,
           (clock_timestamp() - t0)::interval
    FROM public.mv_media_municipio_fazendas;
END;
$$;


CREATE VIEW public.vw_media_municipio_fazendas_fmt AS
SELECT
    mun_cod, regiao, mercado_regional, uf, municipio,
    n_anuncios, n_agricola, n_pecuaria, n_floresta_plantada, n_vegetacao_nativa,
    CASE WHEN mediana_geral IS NULL THEN NULL ELSE translate(to_char(mediana_geral::numeric,'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS mediana_geral,
    CASE WHEN media_geral   IS NULL THEN NULL ELSE translate(to_char(media_geral::numeric,  'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS media_geral,
    CASE WHEN coef_dispersao_geral IS NULL THEN NULL ELSE translate(to_char(coef_dispersao_geral::numeric,'FM990D00'), '.', ',') || ' %' END AS coef_dispersao_geral,

    CASE WHEN mediana_agricola IS NULL THEN NULL ELSE translate(to_char(mediana_agricola::numeric,'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS mediana_agricola,
    CASE WHEN media_agricola   IS NULL THEN NULL ELSE translate(to_char(media_agricola::numeric,  'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS media_agricola,
    CASE WHEN coef_dispersao_agricola IS NULL THEN NULL ELSE translate(to_char(coef_dispersao_agricola::numeric,'FM990D00'), '.', ',') || ' %' END AS coef_dispersao_agricola,

    CASE WHEN mediana_pecuaria IS NULL THEN NULL ELSE translate(to_char(mediana_pecuaria::numeric,'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS mediana_pecuaria,
    CASE WHEN media_pecuaria   IS NULL THEN NULL ELSE translate(to_char(media_pecuaria::numeric,  'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS media_pecuaria,
    CASE WHEN coef_dispersao_pecuaria IS NULL THEN NULL ELSE translate(to_char(coef_dispersao_pecuaria::numeric,'FM990D00'), '.', ',') || ' %' END AS coef_dispersao_pecuaria,

    CASE WHEN mediana_floresta_plantada IS NULL THEN NULL ELSE translate(to_char(mediana_floresta_plantada::numeric,'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS mediana_floresta_plantada,
    CASE WHEN media_floresta_plantada   IS NULL THEN NULL ELSE translate(to_char(media_floresta_plantada::numeric,  'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS media_floresta_plantada,
    CASE WHEN coef_dispersao_floresta_plantada IS NULL THEN NULL ELSE translate(to_char(coef_dispersao_floresta_plantada::numeric,'FM990D00'), '.', ',') || ' %' END AS coef_dispersao_floresta_plantada,

    CASE WHEN mediana_vegetacao_nativa IS NULL THEN NULL ELSE translate(to_char(mediana_vegetacao_nativa::numeric,'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS mediana_vegetacao_nativa,
    CASE WHEN media_vegetacao_nativa   IS NULL THEN NULL ELSE translate(to_char(media_vegetacao_nativa::numeric,  'FM999G999G990D00'), ',.', '.,') || ' R$/ha' END AS media_vegetacao_nativa,
    CASE WHEN coef_dispersao_vegetacao_nativa IS NULL THEN NULL ELSE translate(to_char(coef_dispersao_vegetacao_nativa::numeric,'FM990D00'), '.', ',') || ' %' END AS coef_dispersao_vegetacao_nativa
FROM public.mv_media_municipio_fazendas;

COMMIT;
