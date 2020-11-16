#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

def getRegion3NetCDF4(dirpath, storm):

    url = 'http://tds.renci.org:8080/thredds/fileServer/RegionThree-Solutions/Simulations/'+storm[0:3].upper()+storm[3:len(storm)]+'_X_sh/fort.64.nc'
    os.chdir(dirpath+'nc')

    filename = wget.download(url)
    prefix = "_".join(url.split('/')[7].split('_')[:2]).lower()

    os.rename(filename, dirpath+'nc/'+prefix+'_'+filename)
    #os.rename(filename, dirpath+'nc/'+storm.lower()+'_'+filename)

def createtable(storm, timeinterval):
    tablename = storm.lower()
    try:
        conn = psycopg2.connect("dbname='reg3sim' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

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
        cur.execute("""SELECT create_hypertable(%(table_name)s, 'timestamp', 'node', 2, create_default_indexes=>FALSE,
            chunk_time_interval => interval %(time_interval)s)""",
            {'table_name': tablename, 'time_interval':timeinterval})
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

def ingestData(dirpath, innc):
    if len([f for f in glob.glob(dirpath+"ingest")]) == 0:
        os.mkdir(dirpath+"ingest")

    innc = innc.lower()
    tablename = innc.split('.')[0]+'64'

    with open(dirpath+'ingest/'+tablename+'.csv', 'a') as file:
        file.write('records_ingested,time_lapsed\n')

    file.close()

    with open(dirpath+'ingest/'+tablename+'_1545.csv', 'a') as file:
        file.write('timestamp\n')

    file.close()

    os.chdir(dirpath+'nc')
    if len([f for f in glob.glob("csvfort64")]) == 0:
        os.mkdir("csvfort64")

    with xr.open_dataset(innc, drop_variables=['neta', 'nvel']) as nc:
        startdate = datetime(2000,9,1,0,0,0)

        try:
            dtime = nc.variables['time'][:].data
            lon = nc.variables['x'][:].data
            ncells = len(lon)
        except KeyError:
            Path(dirpath+'ingest/'+tablename+'_missingvars.txt').touch()

            intime = nc.variables['time'][:].data
            dtime = np.empty(0, dtype='datetime64[s]')
            for tstep in intime:
                nstep = np.datetime64(str(startdate + timedelta(seconds=tstep*60*60)))
                dtime= np.append(dtime, nstep)

            shape = nc.variables['u-vel'].shape
            ncells = shape[1]

        ntime = len(dtime)
        node = np.arange(ncells)

        for i in range(ntime):
            dminute = str(dtime[i]).split(':')[1] 

            if dminute == '00' or dminute == '30': 
                start_time = time.time()

                try:
                    u_vel_data = nc.variables['u-vel'][i,:].data
                    v_vel_data = nc.variables['v-vel'][i,:].data
                except RuntimeWarning:
                    sys.exit('*** DeprecationWarning: elementwise comparison failed; this will raise an error in the future.')

                findex = np.where(u_vel_data==min(u_vel_data))
                u_vel_data[findex] = np.nan
                findex = np.where(v_vel_data==min(v_vel_data))
                v_vel_data[findex] = np.nan

                timestamp = np.array([str(dtime[i])] * ncells)

                df = pd.DataFrame({'node': node, 'u_vel': u_vel_data, 'v_vel': v_vel_data, 'timestamp': timestamp}, columns=['node', 'u_vel', 'v_vel', 'timestamp'])

                outcsvfile = "_".join(innc.split('/')[len(innc.split('/'))-1].split('_')[0:2]) + '_' + \
                      str("".join("".join(str(dtime[0]).split('-')).split(':'))) + '.fort.64.csv'
                df.to_csv('csvfort64/'+outcsvfile, encoding='utf-8', header=True, index=False)

                stream = os.popen('timescaledb-parallel-copy --db-name reg3sim --connection "host=localhost user=data password=adcirc sslmode=disable" --table '+tablename+' --file '+'csvfort64/'+outcsvfile+' --skip-header --workers 4 --copy-options "CSV"')
                output = stream.read()

                os.remove('csvfort64/'+outcsvfile)

                stop_time = time.time()
                time_lapsed = stop_time - start_time

                with open(dirpath+'ingest/'+tablename+'.csv', 'a') as file:
                    file.write(output.strip()+','+str(time_lapsed)+'\n')

                file.close()
            else:
                with open(dirpath+'ingest/'+tablename+'_1545.csv', 'a') as file:
                    file.write(str(dtime[i])+'\n')

                file.close()

dirpath = "/home/data/ingestProcessing/"
storm = sys.argv[1]
getRegion3NetCDF4(dirpath, storm)
createtable(storm+"_fort64","2 hour")
ingestData(dirpath, storm+'_fort.64.nc')
