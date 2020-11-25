#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Import modules.
import os, time, glob, wget, sys
import psycopg2, glob
from psycopg2.extensions import AsIs
import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

# Function that downloads netcdf file from RENCI THREDDS server, using wget.
def getRegion3NetCDF4(dirpath, storm):
    """
    Make nc directory, if it does not already exist. The nc directory stores the
    netcdf file downloaded from the RENCI THREDDS server.
    """
    if len([f for f in glob.glob(dirpath+"nc")]) == 0:
        os.mkdir(dirpath+"nc")

    # URL to ADCIRC Region III Simulation fort64 data.
    url = 'http://tds.renci.org:8080/thredds/fileServer/RegionThree-Solutions/Simulations/'+storm[0:3].upper()+storm[3:len(storm)]+'_X_sh/fort.64.nc'
    os.chdir(dirpath+'nc')

    # Download file using wget.
    filename = wget.download(url)
    prefix = "_".join(url.split('/')[7].split('_')[:2]).lower()

    # Write downloaded data, using path and filename.
    os.rename(filename, dirpath+'nc/'+prefix+'_'+filename)
    #os.rename(filename, dirpath+'nc/'+storm.lower()+'_'+filename)

# Create table, in reg3sim database, for storm.
def createtable(storm, timeinterval):
    tablename = storm.lower()
    try:
        # Connect to database using psycopg2.
        conn = psycopg2.connect("dbname='reg3sim' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        # SQL statement that creates table for storm.
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")
        cur.execute("""CREATE TABLE %(table_name)s (
                node INTEGER,
                u_vel NUMERIC,
                v_vel NUMERIC,
                timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                PRIMARY KEY(timestamp, node)
            );""",
        {'table_name': AsIs(tablename)})
        # Create hypertable of newly create table, using time interval.
        cur.execute("""SELECT create_hypertable(%(table_name)s, 'timestamp', 'node', 2, create_default_indexes=>FALSE,
            chunk_time_interval => interval %(time_interval)s)""",
            {'table_name': tablename, 'time_interval':timeinterval})
        # Create index using node and timestamp. This is set up for querying vector tiles. Other indeces could be created.
        cur.execute("""CREATE INDEX ON %(table_name)s (node, timestamp desc)""",
            {'table_name': AsIs(tablename)})
        cur.execute("""CREATE INDEX ON %(table_name)s (timestamp desc, node)""",
            {'table_name': AsIs(tablename)})
        cur.execute("""COMMIT""")
        cur.execute("""ANALYZE %(table_name)s""",
                {'table_name': AsIs(tablename)})

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

# Ingest storm track data into database.
def ingestData(dirpath, innc):
    """
    Make ingest directory, if it does not already exist. The ingest directory stores output
    from the ingest process, showing how long it takes to ingest a timestamp.
    """
    if len([f for f in glob.glob(dirpath+"ingest")]) == 0:
        os.mkdir(dirpath+"ingest")

    # Create table name from netcdf input filename.
    innc = innc.lower()
    tablename = innc.split('.')[0]+'64'

    # Write header for ingest output file.
    with open(dirpath+'ingest/'+tablename+'.csv', 'a') as file:
        file.write('records_ingested,time_lapsed\n')

    file.close()

    with open(dirpath+'ingest/'+tablename+'_1545.csv', 'a') as file:
        file.write('timestamp\n')

    file.close()

    """
    Change directory to netcdf (nc) directory, and create csvfort64 directory,
    if it does not exist. The csvfort64 directory will temporarily store csv
    files that contain data from the netcdf file, for a specific timestamp.
    The file is deleted after it is ingested into the database using
    timescaledb-parallel-copy.
    """
    os.chdir(dirpath+'nc')
    if len([f for f in glob.glob("csvfort64")]) == 0:
        os.mkdir("csvfort64")

    # Open the netcdf file.
    with xr.open_dataset(innc, drop_variables=['neta', 'nvel']) as nc:
        """
        Create start date. This is only needed if the startdate is not already in the netcdf file.
        xarray uses the startdate to convert time in seconds into datetime in the form YYYY-MM-DD hh:mm:ss.
        If the startdate is not in the netcdf file xarray throws a KeyError. In that case the seconds are
        converted to the form YYYY-MM-DD hh:mm:ss, using the startdate defined below. This problem has only
        occurred in a few files, that we later corrected. This code was used before the files were corrected.
        In future ingest this code will not be needed.
        """
        startdate = datetime(2000,9,1,0,0,0)

        try:
            # Get timestamps (datetime), and longitude from netcdf file.
            dtime = nc.variables['time'][:].data
            lon = nc.variables['x'][:].data
            # Calculate number of cells or nodes, from longitude variable.
            ncells = len(lon)
        except KeyError:
            # Create file in the ingest directory indicating that a KeyError was thrown.
            Path(dirpath+'ingest/'+tablename+'_missingvars.txt').touch()

            # Create timestamps in the form YYYY-MM-DD hh:mm:ss, using the startdate defined above.
            intime = nc.variables['time'][:].data
            dtime = np.empty(0, dtype='datetime64[s]')
            for tstep in intime:
                nstep = np.datetime64(str(startdate + timedelta(seconds=tstep*60*60)))
                dtime= np.append(dtime, nstep)

            # Get ncells from u-vel.
            shape = nc.variables['u-vel'].shape
            ncells = shape[1]

        # Calculate number of timestamps to be used in for loop, and create node array from ncells.
        ntime = len(dtime)
        node = np.arange(ncells)

        # For loop of ntime, which will extract data at each timestamp and ingest it into database.
        for i in range(ntime):
            # Get minute value from timestamp.
            dminute = str(dtime[i]).split(':')[1] 

            # Check to see if minute value is 00 or 30. If true process data.
            if dminute == '00' or dminute == '30': 
                # get start_time to use in output to ingest file.
                start_time = time.time()

                try:
                    # get u_vel, and v_vel variables for specific timestamp.
                    u_vel_data = nc.variables['u-vel'][i,:].data
                    v_vel_data = nc.variables['v-vel'][i,:].data
                except RuntimeWarning:
                    sys.exit('*** DeprecationWarning: elementwise comparison failed; this will raise an error in the future.')

                # Convert minimum value of u_vel, and v_vel data to NaN.
                findex = np.where(u_vel_data==min(u_vel_data))
                u_vel_data[findex] = np.nan
                findex = np.where(v_vel_data==min(v_vel_data))
                v_vel_data[findex] = np.nan

                # Create array of timestamp value.
                timestamp = np.array([str(dtime[i])] * ncells)

                # Create DataFrame, and input variables.
                df = pd.DataFrame({'node': node, 'u_vel': u_vel_data, 'v_vel': v_vel_data, 'timestamp': timestamp}, columns=['node', 'u_vel', 'v_vel', 'timestamp'])

                # Create temporary csv file for ingest, and output variables to file.
                outcsvfile = "_".join(innc.split('/')[len(innc.split('/'))-1].split('_')[0:2]) + '_' + \
                      str("".join("".join(str(dtime[0]).split('-')).split(':'))) + '.fort.64.csv'
                df.to_csv('csvfort64/'+outcsvfile, encoding='utf-8', header=True, index=False)

                # Copy csv file to database using timescaledb-parallel-copy.
                # In this case timescaledb-parallel-copy is using 4 CPU's (--workers) to copy the data.
                stream = os.popen('timescaledb-parallel-copy --db-name reg3sim --connection "host=localhost user=data password=adcirc sslmode=disable" --table '+tablename+' --file '+'csvfort64/'+outcsvfile+' --skip-header --workers 4 --copy-options "CSV"')
                output = stream.read()

                # Remove the csv file, after the data has been copied to database.
                os.remove('csvfort64/'+outcsvfile)

                # Get stop time and calculate time lapsed from stop time, and start time.
                stop_time = time.time()
                time_lapsed = stop_time - start_time

                # Output timelapsed to inget output file.
                with open(dirpath+'ingest/'+tablename+'.csv', 'a') as file:
                    file.write(output.strip()+','+str(time_lapsed)+'\n')

                file.close()
            else:
                # If minute value is not 00 or 30, it will be 15 or 45. Output this information to 1545 csv file.
                with open(dirpath+'ingest/'+tablename+'_1545.csv', 'a') as file:
                    file.write(str(dtime[i])+'\n')

                file.close()

# Define ingest processing directory path in container.
dirpath = "/home/data/ingestProcessing/"
# Get storm track name from sys argv input.
storm = sys.argv[1]
# Get netcdf data.
getRegion3NetCDF4(dirpath, storm)
# Create storm  hypertable using 2 hour time interval.
createtable(storm+"_fort64","2 hour")
# Ingest data into database from netcdf file.
ingestData(dirpath, storm+'_fort.64.nc')
