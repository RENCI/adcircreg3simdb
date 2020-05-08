CONTAINER="region3db"
DB="postgres"
TABLE="r3sim_fort_geom"

sudo docker exec -u postgres ${CONTAINER} psql -d ${DB} -c "\COPY ${TABLE} FROM '/var/lib/postgresql/data/csv/Region3Geo.csv' DELIMITER ',' CSV HEADER "

