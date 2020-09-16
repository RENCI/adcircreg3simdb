CREATE OR REPLACE
-- http://localhost:7800/public.region3_sim_storms/13/2362/3138.pbf?stormname=var_dp3r3b1c1h1l1_fort63&timestep=2000-09-05T19:30:00&properties=node,zeta,bathymetry
FUNCTION public.region3_sim_storms(z integer, x integer, y integer, stormtable text, timestep text)
RETURNS bytea
AS $$
DECLARE
  result bytea;
BEGIN
  EXECUTE format(
     'WITH
      bounds AS (
        SELECT ST_TileEnvelope(%s, %s, %s) AS geom 
      ),
      node_ids AS
        (SELECT G.node, G.geom
         FROM r3sim_fort_geom AS G, bounds
         WHERE ST_Intersects(G.geom, ST_Transform(bounds.geom, 4326))),
      mvtgeom AS (
        SELECT T.geom AS geom, S.node AS node, S.zeta AS zeta, T.bathymetry AS bathymetry
        FROM
          (SELECT node, zeta, timestamp
           FROM %s
           WHERE timestamp = %s 
           AND node IN (SELECT node FROM node_ids)) S
          LEFT JOIN
          (SELECT ST_AsMVTGeom(ST_Transform(G.geom, 3857), bounds.geom, 4096, 256, true) AS geom, G.node AS node, G.bathymetry AS bathymetry
           FROM r3sim_fort_geom AS G, bounds
           WHERE ST_Intersects(G.geom, ST_Transform(bounds.geom, 4326))) T
          ON S.node = T.node
      )
      SELECT ST_AsMVT(mvtgeom, %s)
      FROM mvtgeom;', z, x, y, quote_ident(stormtable), quote_literal(timestep), quote_literal('public.region3_sim_storms')
  )
  INTO result;
  RETURN result;
END
$$
LANGUAGE 'plpgsql'
--STABLE
VOLATILE
PARALLEL SAFE;

COMMENT ON FUNCTION public.region3_sim_storms IS 'Given a tile address, storm name, and timestamp query database.';
