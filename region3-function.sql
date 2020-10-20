CREATE OR REPLACE
-- http://adcirc-db.renci.org/public.region3_sim_storms/13/2362/3138.pbf?&stormtable=var_dp3r3b1c1h1l1_fort63&timestep=2000-09-05T19:30:00&properties=node,zeta,bathymetry
FUNCTION public.region3_sim_storms(z integer, x integer, y integer, stormtable text, timestep text)
RETURNS bytea
AS $$
DECLARE
  result bytea;
  startdtype integer = LENGTH (stormtable) - 5;
  enddtype integer = 6;
  stormdtype text = SUBSTRING (stormtable, startdtype, enddtype);
BEGIN
  IF stormdtype = 'fort63' THEN
    EXECUTE format(
       'WITH
        bounds AS (
          SELECT ST_TileEnvelope(%s, %s, %s) AS geomclip, ST_Expand(ST_TileEnvelope(%s, %s, %s), 256) AS geombuf 
        ),
        node_ids AS
          (SELECT G.node, G.geom, G.bathymetry
           FROM r3sim_fort_geom AS G, bounds
           WHERE ST_Intersects(G.geom, ST_Transform(bounds.geombuf, 4326))),
        mvtgeom AS (
          SELECT T.geom AS geom, S.node AS node, S.zeta AS zeta, T.bathymetry AS bathymetry
          FROM
            (SELECT node, zeta, timestamp
             FROM %s
             WHERE timestamp = %s 
             AND node IN (SELECT node FROM node_ids)) S
            LEFT JOIN
            (SELECT ST_AsMVTGeom(ST_Transform(G.geom, 3857), bounds.geomclip, 4096, 256, true) AS geom, G.node AS node, G.bathymetry AS bathymetry
             FROM node_ids AS G, bounds
             WHERE ST_Intersects(G.geom, ST_Transform(bounds.geombuf, 4326))) T
            ON S.node = T.node
        )
        SELECT ST_AsMVT(mvtgeom, %s)
        FROM mvtgeom;', z, x, y, z, x, y, quote_ident(stormtable), quote_literal(timestep), quote_literal('public.region3_sim_storms')
    )
    INTO result;
    RETURN result;
  ELSIF stormdtype = 'swan63' THEN
    EXECUTE format(
       'WITH
        bounds AS (
          SELECT ST_TileEnvelope(%s, %s, %s) AS geomclip, ST_Expand(ST_TileEnvelope(%s, %s, %s), 256) AS geombuf 
        ),
        node_ids AS
          (SELECT G.node, G.geom, G.bathymetry
           FROM r3sim_fort_geom AS G, bounds
           WHERE ST_Intersects(G.geom, ST_Transform(bounds.geombuf, 4326))),
        mvtgeom AS (
          SELECT T.geom AS geom, S.node AS node, S.hs AS hs, S.tps AS tps, S.dir AS dir, T.bathymetry AS bathymetry
          FROM
            (SELECT node, hs, tps, dir, timestamp
             FROM %s
             WHERE timestamp = %s 
             AND node IN (SELECT node FROM node_ids)) S
            LEFT JOIN
            (SELECT ST_AsMVTGeom(ST_Transform(G.geom, 3857), bounds.geomclip, 4096, 256, true) AS geom, G.node AS node, G.bathymetry AS bathymetry
             FROM node_ids AS G, bounds
             WHERE ST_Intersects(G.geom, ST_Transform(bounds.geombuf, 4326))) T
            ON S.node = T.node
        )
        SELECT ST_AsMVT(mvtgeom, %s)
        FROM mvtgeom;', z, x, y, z, x, y, quote_ident(stormtable), quote_literal(timestep), quote_literal('public.region3_sim_storms')
    )
    INTO result;
    RETURN result;
  END IF;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
PARALLEL SAFE;

COMMENT ON FUNCTION public.region3_sim_storms IS 'Given a tile address, storm name, and timestamp query database.';
