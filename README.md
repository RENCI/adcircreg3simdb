# adcircreg3simdb
## ADCIRC Region 3 Simulation Database

#### Build docker images

###### Change directory to build

cd build

###### Then run:

./buildimage.sh  
./createnetwork.sh

###### There are two options for creating the container. The first it to create a container without creating 
###### volumes on /projects/regionthree/. In this case you just run:

./createcontainer.sh

###### The second case is to create volumes on /projects/regionthree/. In that case you edit the createcontainer.sh
###### commenting out the existing container command, and then uncommenting the container command the uses volumes.
 
###### Then you run:

./createcontainer.sh

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
sg postgres -c './createGeoFortZipCSV.py storm_name'  
sg postgres -c './ingestGeoFortCSV.py'  

###### example storm name:  BP1_dp1r1b1c1h1l1
