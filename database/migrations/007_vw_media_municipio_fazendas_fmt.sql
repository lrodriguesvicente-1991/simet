-- =====================================================================
-- 007: vw_media_municipio_fazendas_fmt
-- =====================================================================
-- View-irma da matview mv_media_municipio_fazendas, com os valores
-- monetarios formatados no padrao BR (99.999.999,99 R$/ha) para
-- exportacao direta em CSV/Planilhas. A matview original continua
-- numerica -- a API e o frontend continuam usando ela.
--
-- Uso tipico:
--   \copy (SELECT * FROM vw_media_municipio_fazendas_fmt) TO 'fazendas.csv' CSV HEADER
--   SELECT * FROM vw_media_municipio_fazendas_fmt WHERE uf = 'GO';

BEGIN;

DROP VIEW IF EXISTS public.vw_media_municipio_fazendas_fmt;

CREATE VIEW public.vw_media_municipio_fazendas_fmt AS
WITH fmt AS (
    -- Helper inline: to_char gera '99,999,999.99'. Trocamos para
    -- padrao BR '99.999.999,99' via replace em tres passos.
    SELECT
        mun_cod,
        regiao,
        mercado_regional,
        uf,
        municipio,
        n_anuncios,
        origem,
        mun_cod_vizinho,
        municipio_vizinho,
        nivel_fallback,
        dist_km_vizinho,
        mediana_geral,
        media_geral,
        mediana_agricola,
        media_agricola,
        mediana_pecuaria,
        media_pecuaria,
        mediana_floresta_plantada,
        media_floresta_plantada,
        mediana_vegetacao_nativa,
        media_vegetacao_nativa
    FROM public.mv_media_municipio_fazendas
),
formatar AS (
    SELECT
        f.*,
        -- usa o placeholder `~` para evitar colisao entre separadores durante o swap
        translate(
            to_char(mediana_geral::numeric, 'FM999G999G990D00'),
            ',.', '.,'
        ) AS mediana_geral_br,
        translate(to_char(media_geral::numeric,               'FM999G999G990D00'), ',.', '.,') AS media_geral_br,
        translate(to_char(mediana_agricola::numeric,          'FM999G999G990D00'), ',.', '.,') AS mediana_agricola_br,
        translate(to_char(media_agricola::numeric,            'FM999G999G990D00'), ',.', '.,') AS media_agricola_br,
        translate(to_char(mediana_pecuaria::numeric,          'FM999G999G990D00'), ',.', '.,') AS mediana_pecuaria_br,
        translate(to_char(media_pecuaria::numeric,            'FM999G999G990D00'), ',.', '.,') AS media_pecuaria_br,
        translate(to_char(mediana_floresta_plantada::numeric, 'FM999G999G990D00'), ',.', '.,') AS mediana_floresta_plantada_br,
        translate(to_char(media_floresta_plantada::numeric,   'FM999G999G990D00'), ',.', '.,') AS media_floresta_plantada_br,
        translate(to_char(mediana_vegetacao_nativa::numeric,  'FM999G999G990D00'), ',.', '.,') AS mediana_vegetacao_nativa_br,
        translate(to_char(media_vegetacao_nativa::numeric,    'FM999G999G990D00'), ',.', '.,') AS media_vegetacao_nativa_br
    FROM fmt f
)
SELECT
    mun_cod,
    regiao,
    mercado_regional,
    uf,
    municipio,
    n_anuncios,
    origem,
    mun_cod_vizinho,
    municipio_vizinho,
    nivel_fallback,
    dist_km_vizinho,
    CASE WHEN mediana_geral             IS NULL THEN NULL ELSE mediana_geral_br             || ' R$/ha' END AS mediana_geral,
    CASE WHEN media_geral               IS NULL THEN NULL ELSE media_geral_br               || ' R$/ha' END AS media_geral,
    CASE WHEN mediana_agricola          IS NULL THEN NULL ELSE mediana_agricola_br          || ' R$/ha' END AS mediana_agricola,
    CASE WHEN media_agricola            IS NULL THEN NULL ELSE media_agricola_br            || ' R$/ha' END AS media_agricola,
    CASE WHEN mediana_pecuaria          IS NULL THEN NULL ELSE mediana_pecuaria_br          || ' R$/ha' END AS mediana_pecuaria,
    CASE WHEN media_pecuaria            IS NULL THEN NULL ELSE media_pecuaria_br            || ' R$/ha' END AS media_pecuaria,
    CASE WHEN mediana_floresta_plantada IS NULL THEN NULL ELSE mediana_floresta_plantada_br || ' R$/ha' END AS mediana_floresta_plantada,
    CASE WHEN media_floresta_plantada   IS NULL THEN NULL ELSE media_floresta_plantada_br   || ' R$/ha' END AS media_floresta_plantada,
    CASE WHEN mediana_vegetacao_nativa  IS NULL THEN NULL ELSE mediana_vegetacao_nativa_br  || ' R$/ha' END AS mediana_vegetacao_nativa,
    CASE WHEN media_vegetacao_nativa    IS NULL THEN NULL ELSE media_vegetacao_nativa_br    || ' R$/ha' END AS media_vegetacao_nativa
FROM formatar;

COMMIT;
