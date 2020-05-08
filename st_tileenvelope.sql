CREATE OR REPLACE
FUNCTION ST_TileEnvelope(z integer, x integer, y integer)
RETURNS geometry
AS $$
  DECLARE
    size float8;
    zp integer = pow(2, z);
    gx float8;
    gy float8;
  BEGIN
    IF y >= zp OR y < 0 OR x >= zp OR x < 0 THEN
        RAISE EXCEPTION 'invalid tile coordinate (%, %, %)', z, x, y;
    END IF;
    size := 40075016.6855784 / zp;
    gx := (size * x) - (40075016.6855784/2);
    gy := (40075016.6855784/2) - (size * y);
    RETURN ST_SetSRID(ST_MakeEnvelope(gx, gy, gx + size, gy - size), 3857);
  END;
$$
LANGUAGE 'plpgsql'
IMMUTABLE
STRICT
PARALLEL SAFE;
