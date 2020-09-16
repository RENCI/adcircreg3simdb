# set the base image
FROM continuumio/anaconda3

# author
MAINTAINER Jim McManus

# extra metadata
LABEL version="1.0"
LABEL description="regin3db image with Dockerfile."

# Set user environment info, USER_ID and USER_GID are 1324, and PG_ID and PG_GID are 70 on adcric-db
ENV USER=data GROUP=data USER_ID=1324 USER_GID=1324 PASSWORD=adcircdata CONDAENV=adcirc PG_USER=postgres PG_GROUP=postgres PG_ID=70 PG_GID=70 PG_PASSWORD=postgres

# update sources list
# install basic apps, one per line for better caching
RUN apt-get clean && apt-get update &&\ 
    apt-get install -qy nano \
    curl \
    vim \
    lsb-release \
    gnupg2 \
    sudo

# add user data, specific to adcirc-db and general setup, respectively
RUN groupadd -r -g $USER_GID $GROUP && useradd --no-log-init -r -u $USER_ID -g $USER_GID $USER 

# define working directory
WORKDIR /home/$USER

# Make RUN commands use `bash --login`:
SHELL ["/bin/bash", "--login", "-c"]

# Create the environment, and initialize conda in bash config files.
# Activate the environment, and install 1.44.1 packages:
COPY environment.yml .
RUN conda env create -f environment.yml &&\
 conda init bash &&\
 conda activate $CONDAENV &&\
 conda update -n base -c defaults conda -y --quiet

# Copy bashrc to home directory as .bashrc
ADD bashrc /home/$USER/.bashrc

# restart postgresql, make password for $USER, 
# clone adcircreg3simdb repository, and 
# change owner and group to data for /home/data
RUN  echo -e "$PASSWORD\n$PASSWORD" | passwd $USER &&\
  mkdir -p /home/$USER/ingestProcessing/csv &&\
  mkdir -p /home/$USER/ingestProcessing/nc &&\
  git clone https://github.com/RENCI/adcircreg3simdb.git &&\
  chown -R $USER:$GROUP /home/$USER

# add user postgres for adcirc-db
# get postgres, postgis and timescaledb repos
RUN groupadd -r -g $PG_GID $PG_GROUP && useradd --no-log-init -r -u $PG_ID -g $PG_GID $PG_USER &&\
  usermod -a -G $PG_GROUP data &&\
  apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8 &&\
  echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -c -s)-pgdg main" | tee /etc/apt/sources.list.d/pgdg.list &&\
  wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - &&\
  sh -c "echo 'deb https://packagecloud.io/timescale/timescaledb/debian/ `lsb_release -c -s` main' > /etc/apt/sources.list.d/timescaledb.list"  &&\
  wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | apt-key add - 

# install postgres, postgis and timescaledb
RUN apt-get update && apt-get install -qy postgresql-11-postgis-3 \
    timescaledb-postgresql-11 &&\
    apt-get -qy autoremove

# add user postgres
USER $PG_USER

# add timescaledb to postgresql.conf, and then
# Create a PostgreSQL role named ``postgres`` with ``postgres`` as the password and
# then create a database `postgres` owned by the ``postgres`` role.
# Note: here we use ``&&\`` to run commands one after the other - the ``\``
#       allows the RUN command to span multiple lines.
RUN echo "shared_preload_libraries = 'timescaledb'" >> /etc/postgresql/11/main/postgresql.conf &&\
    /etc/init.d/postgresql start &&\
    psql --command "ALTER USER "$PG_USER" PASSWORD '"$PG_PASSWORD"';" &&\
    psql --command "CREATE DATABASE reg3sim" &&\
    psql -d reg3sim --command "CREATE EXTENSION postgis;" &&\
    psql -d reg3sim --command "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;" &&\
    psql -d reg3sim --command "CREATE USER data WITH ENCRYPTED PASSWORD 'adcirc';" &&\
    psql -d reg3sim --command "GRANT ALL PRIVILEGES ON DATABASE reg3sim TO data;" 
    #psql -d reg3sim --command -f /home/data/adcircreg3simdb/region3-function.sql


USER root 

# tune tilescaledb. 
# Adjust PostgreSQL configuration so that remote connections to the
# database are possible.
# And add ``listen_addresses`` to ``/etc/postgresql/11/main/postgresql.conf``
RUN timescaledb-tune --quiet --yes &&\
  echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/11/main/pg_hba.conf &&\
  echo "listen_addresses='*'" >> /etc/postgresql/11/main/postgresql.conf

# Expose the PostgreSQL port
EXPOSE 5432
 
# Add VOLUMEs to allow backup of config, logs and databases
VOLUME  ["/etc/postgresql", "/var/log/postgresql", "/var/lib/postgresql", "/home/data"]
 
# Set the default command to run when starting the container
CMD ["/usr/lib/postgresql/11/bin/postgres", "-D", "/var/lib/postgresql/11/main", "-c", "config_file=/etc/postgresql/11/main/postgresql.conf"]

# locales to UTF-8
RUN locale-gen C.UTF-8 && /usr/sbin/update-locale LANG=C.UTF-8
ENV LC_ALL C.UTF-8
