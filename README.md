# adcircreg3simdb
## ADCIRC Region 3 Simulation Database

#### Build docker images

###### Change directory to build

cd build

###### You will also need to remove comment out on the line:

RUN useradd --no-log-init -r -u 1324 data

###### Edit the Docker file, and comment out the line:

RUN useradd --no-log-init -r data

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
cd /var/local/postgresql  
mv /var/lib/postgresql/11 .  
vi /etc/postgresql/11/main/postgresql.conf  
change data_directory from /var/lib/postgresql to /var/local/postgresql

###### This will change the postgresql data directory to /var/local/postgresql utilizing the disk space there.  

#### Next you will need to restart postgresql as root:

###### Start the postgresql server

service postgresql restart

#### Your will then need to set up data processing as user data

###### su to data and run bash

su data  
bash

###### Install Anaconda

cd /home/data  
mkdir tmp  
cd tmp  
curl -O https://repo.anaconda.com/archive/Anaconda3-2020.02-Linux-x86_64.sh > Anaconda3-2020.02-Linux-x86_64.sh  
bash Anaconda3-2020.02-Linux-x86_64.sh  
cd ..  
source .bashrc  
rm -r tmp  

###### Install netcdf and xarray using conda

conda install -c anaconda netcdf4  
conda install -c anaconda xarray  

###### Install psycopg2 and wget using pip

pip install psycopg2-binary  
pip install wget  

###### Set up Git and clone adcircreg3simdb

git config --global user.email "jmpmcman@renci.org"  
git config --global user.name "jmpmcmanus"  

ssh-keygen -t rsa  
add id_rsa.pub to https://github.com/yourgithubaccount keys  
git clone git@github.com:RENCI/adcircreg3simdb.git    

###### Begin processing data

mkdir nc csv  
cd adcircreg3simdb   
sg postgres -c './ingestZetaFortNcCSV.py storm_name'  
sg postgres -c './createGeoFortZipCSV.py storm_name'  
sg postgres -c './ingestGeoFortCSV.py'  

###### example storm name:  BP1_dp1r1b1c1h1l1
