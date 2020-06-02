psql -U data -d reg3sim -f '/home/data/adcircreg3simdb/region3db_fort_geo.sql'
psql -U data -d reg3sim -c "\COPY r3sim_fort_geom FROM '/home/data/csv/Region3Geo.csv' DELIMITER ',' CSV HEADER"
psql -U data -d reg3sim -f '/home/data/adcircreg3simdb/region3db_fort_geom.sql'
