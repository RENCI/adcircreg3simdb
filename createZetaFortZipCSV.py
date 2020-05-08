#!/home/data/anaconda3/bin/python
# -*- coding: utf-8 -*-

import os, glob
import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime
from zipfile import ZipFile
import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
#warnings.filterwarnings("error")

def createZipFile(dirpath, infile):
    os.chdir(dirpath+"zip/")
    if len([f for f in glob.glob("csvfort")]) == 0:
        os.mkdir("csvfort")

    with xr.open_dataset(infile) as nc:
        startdate = datetime(2000,9,1,0,0,0)
        startsecond = datetime.timestamp(startdate)

        time = nc.variables['time'][:].data
        lon =nc.variables['x'][:].data

        ntime = len(time)
        ncells = len(lon)
        node = np.arange(ncells)

        for i in range(ntime):
            outzipfile = ZipFile(".".join(infile.split('/')[len(infile.split('/'))-1].split('.')[0:2])+'.zip','a')

            try:
                zeta = nc.variables['zeta'][i,:]
            except RuntimeWarning:
                outzipfile.close()
                sys.exit('*** DeprecationWarning: elementwise comparison failed; this will raise an error in the future.')

            zeta_data = zeta.data
            findex = np.where(zeta_data==min(zeta_data))
            zeta_data[findex] = np.nan

            timestamp = np.array([str(time[i])] * ncells)

            df = pd.DataFrame({'node': node, 'zeta': zeta_data, 'timestamp': timestamp}, columns=['node', 'zeta', 'timestamp'])

            outcsvfile = "_".join(infile.split('/')[len(infile.split('/'))-1].split('_')[0:2]) + '_' + \
                  str(time[i]) + '.fort.63_mod.csv'
            df.to_csv('csvfort/'+outcsvfile, encoding='utf-8', header=True, index=False)
            outzipfile.write('csvfort/'+outcsvfile)
            os.remove('csvfort/'+outcsvfile)

            outzipfile.close()

dirpath = '/home/data/'
infiles = [f for f in glob.glob(dirpath+"nc/"+"*.nc")]
infiles.sort()
infile = infiles[0]
#for infile in infiles:
createZipFile(dirpath, infile.strip())

