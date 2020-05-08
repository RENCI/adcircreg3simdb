# set the base image
FROM debian

# author
MAINTAINER Jim McManus

# extra metadata
LABEL version="1.0"
LABEL description="regin3db image with Dockerfile."

# update sources list
RUN apt-get clean

# install basic apps, one per line for better caching
RUN apt-get update && apt-get install -qy git \
    locales \
    nano \
    tmux \
    wget \
    curl \
    vim \
    lsb-release \
    gnupg2 \
    unzip 

# add user postgres for adcirc-db
#RUN groupadd -r -g 70 postgres && useradd --no-log-init -r -u 70 -g 70 postgres

# get postgres, postgis and timescaledb repos
RUN apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -c -s)-pgdg main" | tee /etc/apt/sources.list.d/pgdg.list
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN sh -c "echo 'deb https://packagecloud.io/timescale/timescaledb/debian/ `lsb_release -c -s` main' > /etc/apt/sources.list.d/timescaledb.list"
RUN wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | apt-key add -

# install postgres, postgis and timescaledb
RUN apt-get update && apt-get install -qy postgresql-11-postgis-3 \
    timescaledb-postgresql-11

# cleanup
RUN apt-get -qy autoremove

# add scripts to the container
ADD .bashrc /root/.bashrc
ADD .profile /root/.profile

# add user data, specific to adcirc-db and general setup, respectively
#RUN useradd --no-log-init -r -u 1324 data
RUN useradd --no-log-init -r data
RUN usermod -a -G postgres data

# add user postgres
USER postgres

# add timescaledb to postgresql.conf
RUN echo "shared_preload_libraries = 'timescaledb'" >> /etc/postgresql/11/main/postgresql.conf

# Create a PostgreSQL role named ``postgres`` with ``postgres`` as the password and
# then create a database `postgres` owned by the ``postgres`` role.
# Note: here we use ``&&\`` to run commands one after the other - the ``\``
#       allows the RUN command to span multiple lines.
RUN /etc/init.d/postgresql start &&\
    psql --command "ALTER USER postgres PASSWORD 'postgres';" &&\
    psql --command "CREATE EXTENSION postgis;" &&\
    psql --command "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;" &&\
    psql --command "create user data with encrypted password 'adcirc';" &&\
    psql --command "grant all privileges on database postgres to data;" 

USER root 
# tune tilescaledb
RUN timescaledb-tune --quiet --yes

# Adjust PostgreSQL configuration so that remote connections to the
# database are possible.
RUN echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/11/main/pg_hba.conf
 
# And add ``listen_addresses`` to ``/etc/postgresql/11/main/postgresql.conf``
RUN echo "listen_addresses='*'" >> /etc/postgresql/11/main/postgresql.conf

# Expose the PostgreSQL port
EXPOSE 5432
 
# Add VOLUMEs to allow backup of config, logs and databases
VOLUME  ["/etc/postgresql", "/var/log/postgresql", "/var/lib/postgresql", "/home/data"]
 
# Set the default command to run when starting the container
CMD ["/usr/lib/postgresql/11/bin/postgres", "-D", "/var/lib/postgresql/11/main", "-c", "config_file=/etc/postgresql/11/main/postgresql.conf"]

# change owner and group to data for /home/data
RUN chown -R data:data /home/data

# locales to UTF-8
RUN locale-gen C.UTF-8 && /usr/sbin/update-locale LANG=C.UTF-8
ENV LC_ALL C.UTF-8

# restart postgresql
RUN service postgresql restart
