#!/bin/bash
# Create region3db container, requires a DIRPATH as input

DIRPATH=$1 # One adcirc-db DIRPATH is /projects/regionthree 
PROCESSING=$DIRPATH/ingestProcessing
STORAGE=$DIRPATH/dockerstorage

docker run -ti --name region3db_container --shm-size=4g \
   --network region3db_network \
   -p 5432:5432 \
   --volume $PROCESSING:/home/data/ingestProcessing \
   --volume $STORAGE:/var/local/postgresql \
   -d region3db_image /bin/bash

