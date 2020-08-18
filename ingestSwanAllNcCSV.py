#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, time, glob, wget, sys, psycopg2, pdb
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
    urls = ['http://tds.renci.org:8080/thredds/fileServer/RegionThree-Solutions/Simulations/'+storm[0:3].upper()+storm[3:len(storm)]+'_X_sh/swan_HS.63_mod.nc','http://tds.renci.org:8080/thredds/fileServer/RegionThree-Solutions/Simulations/'+storm[0:3].upper()+storm[3:len(storm)]+'_X_sh/swan_RTP_mod.63.nc','http://tds.renci.org:8080/thredds/fileServer/RegionThree-Solutions/Simulations/'+storm[0:3].upper()+storm[3:len(storm)]+'_X_sh/swan_DIR.63_mod.nc']
    os.chdir(dirpath+'nc/')

    for url in urls:
        filename = wget.download(url)
        prefix = "_".join(url.split('/')[7].split('_')[:2]).lower()

        os.rename(filename,dirpath+'nc/'+prefix+'_'+filename)

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
                hs NUMERIC,
                rtp NUMERIC,
                dir NUMERIC,
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

def ingestData(dirpath,storm,filesuffixes):
    if len([f for f in glob.glob(dirpath+"ingest")]) == 0:
        os.mkdir(dirpath+"ingest")

    storm = storm.lower()
    tablename = storm+'_swan'+'63'

    with open(dirpath+'ingest/'+tablename+'.csv', 'a') as file:
        file.write('records_ingested,time_lapsed\n')

    file.close()

    os.chdir(dirpath+'nc')
    if len([f for f in glob.glob("csvswan")]) == 0:
        os.mkdir("csvswan")

    inncs = [dirpath+'nc/'+storm+filesuffixes[0],dirpath+'nc/'+storm+filesuffixes[1],dirpath+'nc/'+storm+filesuffixes[2]]

    with xr.open_dataset(inncs[0]) as nc0:
        nc1 = xr.open_dataset(inncs[1])
        nc2 = xr.open_dataset(inncs[2])

        try:
            dsecond = nc0.variables['time'][:].data
            dtime = pd.to_datetime(dsecond, unit='s', origin=pd.Timestamp('2000-09-01'))
            lon = nc0.variables['x'][:].data
            ncells = len(lon)
        except KeyError:
            Path(dirpath+'ingest/'+tablename+'_missingvars.txt').touch()

            intime = nc0.variables['time'][:].data
            dtime = np.empty(0, dtype='datetime64[s]')
            for tstep in intime:
                nstep = np.datetime64(str(startdate + timedelta(seconds=tstep*60*60)))
                dtime= np.append(dtime, nstep)

            shape = nc0.variables['hs'].shape
            ncells = shape[1]

        ntime = len(dtime)
        node = np.arange(ncells)

        for i in range(ntime):
            start_time = time.time()

            try:
                hs_data = nc0.variables['hs'][i,:].data
                rtp_data = nc1.variables['rtp'][i,:].data
                dir_data = nc2.variables['dir'][i,:].data
            except RuntimeWarning:
                sys.exit('*** DeprecationWarning: elementwise comparison failed; this will raise an error in the future.')

            findex = np.where(hs_data==min(hs_data))
            hs_data[findex] = np.nan
            findex = np.where(rtp_data==min(rtp_data))
            rtp_data[findex] = np.nan
            findex = np.where(dir_data==min(dir_data))
            dir_data[findex] = np.nan

            timestamp = np.array([str(dtime[i])] * ncells)

            df = pd.DataFrame({'node': node, 'hs': hs_data, 'rtp': rtp_data, 'dir': dir_data, 'timestamp': timestamp}, columns=['node', 'hs', 'rtp', 'dir', 'timestamp'])

            outcsvfile = "_".join(inncs[0].split('/')[-1].split('_')[0:2]) + '_' + \
                  "T".join(str("".join("".join(str(dtime[0]).split('-')).split(':'))).split(' ')) + '.swan.63_mod.csv'
            df.to_csv('csvswan/'+outcsvfile, encoding='utf-8', header=True, index=False)

            stream = os.popen('timescaledb-parallel-copy --db-name reg3sim --connection "host=localhost user=data password=adcirc sslmode=disable" --table '+tablename+' --file '+'csvswan/'+outcsvfile+' --skip-header --workers 4 --copy-options "CSV"')
            output = stream.read()

            os.remove('csvswan/'+outcsvfile)

            stop_time = time.time()
            time_lapsed = stop_time - start_time

            with open(dirpath+'ingest/'+tablename+'.csv', 'a') as file:
                file.write(output.strip()+','+str(time_lapsed)+'\n')

            file.close()

dirpath = "/home/data/ingestProcessing/"
storm = sys.argv[1]
#getRegion3NetCDF4(dirpath,storm)
#createtable(storm+'_swan63',"2 hour")
filesuffixes = ['_swan_HS.63_mod.nc','_swan_RTP_mod.63.nc','_swan_DIR.63_mod.nc']
ingestData(dirpath,storm,filesuffixes)
