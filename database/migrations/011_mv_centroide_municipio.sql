-- =====================================================================
-- 011: Matview de centroides de municipio (lat/lon pre-calculados)
-- =====================================================================
-- Motivo: o /api/dados estava chamando ST_Centroid sobre smt_malha_municipal
-- (geometrias com milhares de vertices cada) em runtime para ~5.500 municipios
-- a cada request, custando ~3-5s so no PostGIS. Persistindo os centroides
-- numa matview indexada o JOIN vira lookup constante e o tempo da query cai
-- para sub-segundo.
--
-- Refresh: junto do refresh da mv_estatisticas_simet em /api/sincronizar.

BEGIN;

DROP MATERIALIZED VIEW IF EXISTS public.mv_centroide_municipio;

CREATE MATERIALIZED VIEW public.mv_centroide_municipio AS
SELECT
    mlm_cd_mun::integer AS mun_cod,
    ST_Y(ST_Centroid(mlm_geom::geometry)) AS lat,
    ST_X(ST_Centroid(mlm_geom::geometry)) AS lon
FROM public.smt_malha_municipal
WHERE mlm_geom IS NOT NULL
WITH DATA;

-- UNIQUE INDEX e prerequisito para REFRESH MATERIALIZED VIEW CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_centroide_mun_pk
    ON public.mv_centroide_municipio (mun_cod);

COMMIT;
