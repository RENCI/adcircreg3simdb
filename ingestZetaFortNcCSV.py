#!/home/data/anaconda3/bin/python
# -*- coding: utf-8 -*-

import os, time, glob
import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

def ingestData(dirpath,innc):
    if len([f for f in glob.glob("/home/data/ingest")]) == 0:
        os.mkdir("/home/data/ingest")

    tablename = innc.split('.')[0].lower()

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
        lon =nc.variables['x'][:].data

        ntime = len(dtime)
        ncells = len(lon)
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

            stream = os.popen('timescaledb-parallel-copy --db-name postgres --connection "host=localhost user=data password=adcirc sslmode=disable" --table '+tablename+' --file '+'csvfort/'+outcsvfile+' --skip-header --workers 4 --copy-options "CSV"')
            output = stream.read()

            os.remove('csvfort/'+outcsvfile)

            stop_time = time.time()
            time_lapsed = stop_time - start_time

            with open('/home/data/ingest/'+tablename+'.csv', 'a') as file:
                file.write(output.strip()+','+str(time_lapsed)+'\n')

            file.close()

dirpath = "/home/data/nc/"
infiles = [f for f in glob.glob(dirpath+"*.nc")]
infiles.sort()
dirlength = len(dirpath)

for infile in infiles:
    innc = infile[dirlength:] 
    ingestData(dirpath,innc)

