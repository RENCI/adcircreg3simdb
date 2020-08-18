# adcircreg3simdb
## ADCIRC Region 3 Simulation Database

#### Build docker images

###### Change directory to build

cd build

###### Then run:

./buildimage.sh  
./createnetwork.sh

###### When creating the container you need to define the volume where the postgresql data, and output data will be 
###### written too. This disk space for this volume needs to large enough to write all of the Region 3 simulation 
###### data. At RENCI we are using /projects/regionthree/ which is accessible on the dcirc-db.edc.renci.org VM.
###### You can use you own directory path. To create the container you run the following command using you own
###### directory path:

./createcontainer.sh /projects/regionthree

###### Your now should be able to access the container shell using the following command:

docker exec -it region3db_container bash

#### If you are using the container with volumes you will need to issue the following commands as postgres:

su postgres 
bash  
cd /var/local/postgresql  
mv /var/lib/postgresql/11 .  
vi /etc/postgresql/11/main/postgresql.conf  
change data_directory from /var/lib/postgresql to /var/local/postgresql

###### This will change the postgresql data directory to /var/local/postgresql utilizing the disk space there.  

#### Next you will need to restart postgresql as root:

###### Start the postgresql server

service postgresql restart

###### After exiting from postgresql and root you can logon to the data account with the folowing command:

docker exec -it --user data region3db_container bash

###### You can run the ingest scripts with the following commands:

cd adcircreg3simdb   
conda activate adcirc  
sg postgres -c './ingestZetaFortNcCSV.py storm_name'
sg postgres -c './ingestSwanAllNcCSV.py storm_name'  
sg postgres -c './createGeoFortZipCSV.py storm_name'  
sg postgres -c './ingestGeoFortCSV.py'  

###### example storm name:  BP1_dp1r1b1c1h1l1
