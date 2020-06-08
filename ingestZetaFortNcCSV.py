#!/home/data/anaconda3/bin/python
# -*- coding: utf-8 -*-

import os, time, glob, wget, sys
import psycopg2, glob
from psycopg2.extensions import AsIs
import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")


def getRegion3NetCDF4(storm):

    url = 'http://tds.renci.org:8080/thredds/fileServer/RegionThree-Solutions/Simulations/'+storm[0:3].upper()+storm[3:len(storm)]+'_X_sh/fort.63_mod.nc'
    os.chdir('/home/data/nc/')

    filename = wget.download(url)
    prefix = "_".join(url.split('/')[7].split('_')[:2]).lower()

    os.rename(filename,'/home/data/nc/'+prefix+'_'+filename)

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
                zeta NUMERIC,
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

def ingestData(dirpath,innc):
    if len([f for f in glob.glob("/home/data/ingest")]) == 0:
        os.mkdir("/home/data/ingest")

    innc = innc.lower()
    tablename = innc.split('.')[0]+'63'

    with open('/home/data/ingest/'+tablename+'.csv', 'a') as file:
        file.write('records_ingested,time_lapsed\n')

    file.close()

    os.chdir(dirpath)
    if len([f for f in glob.glob("csvfort")]) == 0:
        os.mkdir("csvfort")

    with xr.open_dataset(innc) as nc:
        startdate = datetime(2000,9,1,0,0,0)
        startsecond = datetime.timestamp(startdate)

        dtime = nc.variables['time'][:].data
        #lon = nc.variables['x'][:].data
        shape = nc.variables['zeta'].shape

        ntime = len(dtime)
        ncells = shape[1]
        node = np.arange(ncells)

        for i in range(ntime):
            start_time = time.time()

            try:
                zeta = nc.variables['zeta'][i,:]
            except RuntimeWarning:
                sys.exit('*** DeprecationWarning: elementwise comparison failed; this will raise an error in the future.')

            zeta_data = zeta.data
            findex = np.where(zeta_data==min(zeta_data))
            zeta_data[findex] = np.nan

            timestamp = np.array([str(dtime[i])] * ncells)

            df = pd.DataFrame({'node': node, 'zeta': zeta_data, 'timestamp': timestamp}, columns=['node', 'zeta', 'timestamp'])

            outcsvfile = "_".join(innc.split('/')[len(innc.split('/'))-1].split('_')[0:2]) + '_' + \
                  str(dtime[i]) + '.fort.63_mod.csv'
            df.to_csv('csvfort/'+outcsvfile, encoding='utf-8', header=True, index=False)

            stream = os.popen('timescaledb-parallel-copy --db-name reg3sim --connection "host=localhost user=data password=adcirc sslmode=disable" --table '+tablename+' --file '+'csvfort/'+outcsvfile+' --skip-header --workers 4 --copy-options "CSV"')
            output = stream.read()

            os.remove('csvfort/'+outcsvfile)

            stop_time = time.time()
            time_lapsed = stop_time - start_time

            with open('/home/data/ingest/'+tablename+'.csv', 'a') as file:
                file.write(output.strip()+','+str(time_lapsed)+'\n')

            file.close()

dirpath = "/home/data/nc/"
#infiles = [f for f in glob.glob(dirpath+"*.nc")]
#infiles.sort()
storm = sys.argv[1]
getRegion3NetCDF4(storm)
#innc = infile[dirlength:]
createtable(storm+'_fort63',"2 hour")
ingestData(dirpath,storm+'_fort.63_mod.nc')

#for infile in infiles:
#    innc = infile[dirlength:] 
#    ingestData(dirpath,innc)

