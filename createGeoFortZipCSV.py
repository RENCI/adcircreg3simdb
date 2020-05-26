#!/home/data/anaconda3/bin/python
# -*- coding: utf-8 -*-

import os, sys
import xarray as xr
import pandas as pd
import numpy as np

def createZipFile(dirpath, infile):
    with xr.open_dataset(dirpath+'nc/'+infile) as nc:
        lon =nc.variables['x'][:].data
        lat = nc.variables['y'][:].data
        bathymetry = nc.variables['depth'][:].data

        ncells = len(lon)
        node = np.arange(ncells)

        df = pd.DataFrame({'node': node, 'lon': lon, 'lat': lat, 'bathymetry': bathymetry}, columns=['node', 'lon', 'lat', 'bathymetry'])

        outcsvfile = dirpath+'csv/Region3Geo.csv'
        df.to_csv(outcsvfile, encoding='utf-8', header=True, index=False)


dirpath = '/home/data/'
#infile = 'bp1_dp1r2b1c2h1l1_fort.63_mod.nc'
infile = sys.argv[1]
createZipFile(dirpath, infile)

