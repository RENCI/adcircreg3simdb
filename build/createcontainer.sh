# General setup
docker run -it --name region3db_container --shm-size=4g \
   --network region3db_network \
   -p 5432:5432 \
   -d region3db_image /bin/bash

# setup specific to adcirc-db
#docker run -ti --name region3db_container --shm-size=4g
#   --network region3db_network \
#   -p 5434:5432 \
#   --volume /projects/regionthree/ingestProcessing:/home/data \
#   --volume /projects/regionthree/dockerstorage:/var/local/postgresql \
#  -d region3db_image /bin/bash

