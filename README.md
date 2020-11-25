# adcircreg3simdb
## ADCIRC Region 3 Simulation Database

#### Build docker images

##### Change directory to build

cd build

The Dockerfile is currently setup for adcirc-db.edc.renci.org at RENCI. In the Dockerfile USER_ID and USER_GID are given
the value of 1324, and PG_ID and PG_GID are given the value of 70. This has been done to enable write permission to the
storage area defined by the volume, when creating the container. At RENCI the storage area is a shared area, and USER_GID
70 is the group that is used to share the disk. This step may not be necessary if the person who creates the docker image 
and the container also owns the disk space defined by volume. If this is the case the user can edit the Docker file using 
their own user id and group id for these values.

ENV USER=data GROUP=data USER_ID=???? USER_GID=???? PASSWORD=adcircdata CONDAENV=adcirc PG_USER=postgres PG_GROUP=postgres PG_ID=?? PG_GID=?? PG_PASSWORD=postgres

##### After editing the Docker file then run:

./buildimage.sh  
./createnetwork.sh

When creating the container you need to define the volume where the postgresql data, and output data will be 
written too. This disk space for this volume needs to be large enough to write all of the Region 3 simulation 
data. At RENCI we are using /projects/regionthree/ which is accessible on the dcirc-db.edc.renci.org VM.
You can use your own directory path. However, make sure you create the "ingestProcessing" and "dockerstorage" 
directories:  

	mkdir /your/directory/path/ingestProcessing  
        mkdir /your/dirctory/path/dockerstorage  

before creating the container, otherwise docker will create these directories as root. To create 
the container you run the following command using you own
directory path:  

./createcontainer.sh /your/directory/path  

##### You now should be able to access the container shell using the following command:

docker exec -it region3db_container bash

#### If you are using the container with volumes you will need to issue the following commands as postgres:

su postgres 
bash  
cd /var/local/postgresql  
mv /var/lib/postgresql/11 .  
vi /etc/postgresql/11/main/postgresql.conf  
change data_directory from /var/lib/postgresql to /var/local/postgresql

##### This will change the postgresql data directory to /var/local/postgresql utilizing the disk space there.  

#### Next you will need to restart postgresql as user postgres:

##### Start the postgresql server

service postgresql restart

##### Then run a psql command (password is postgres) to load region3-function.sql into the database:

psql -U postgres --password -d reg3sim -p 5432 -h localhost -f /home/data/adcircreg3simdb/region3-function.sql

##### After exiting from postgresql and root you can logon to the data account with the following command:

docker exec -it --user data region3db_container bash

##### You can run the ingest scripts with the following commands:

cd adcircreg3simdb   
conda activate adcirc  
sg postgres -c './ingestZetaFortNcCSV.py storm_name'  
sg postgres -c './ingestSwanAllNcCSV.py storm_name'  
sg postgres -c './ingestVelFortNcCSV.py storm_name'  
sg postgres -c './createGeoFortZipCSV.py storm_name'  
sg postgres -c './ingestGeoFortCSV.py'  

##### example storm name:  BP1_dp1r1b1c1h1l1
