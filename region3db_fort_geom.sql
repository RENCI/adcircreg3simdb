ALTER TABLE r3sim_fort_geom ADD COLUMN geom geometry(POINT,4326);
CREATE INDEX r3sim_fort_geom_index ON r3sim_fort_geom USING SPGIST ( geom );
UPDATE r3sim_fort_geom SET geom = ST_SetSRID(ST_MakePoint(lon,lat),4326);
