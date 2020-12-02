#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Import modules
import os, sys, glob
import xarray as xr
import pandas as pd
import numpy as np

"""
Function that extracts longitude latitude, and bathymetry values from netcdf file, 
and outputs them to csv file along with node values.
"""
def createCSVFile(dirpath, infile):
    """
    Make csv directory, if it does not already exist. The csv directory stores the
    file created by this program.
    """
    if len([f for f in glob.glob(dirpath+"csv")]) == 0:
        os.mkdir(dirpath+"csv")

    # Read input netcdf file
    with xr.open_dataset(dirpath+'nc/'+infile) as nc:
        # Get longitude, latitude and bathymetry values from netcdf file.
        lon =nc.variables['x'][:].data
        lat = nc.variables['y'][:].data
        bathymetry = nc.variables['depth'][:].data

        # Create node array from number of cels in longitude.
        ncells = len(lon)
        node = np.arange(ncells)

        # Create DataFrame and imput variables
        df = pd.DataFrame({'node': node, 'lon': lon, 'lat': lat, 'bathymetry': bathymetry}, columns=['node', 'lon', 'lat', 'bathymetry'])

        # Write DataFrame to netcdf file.
        outcsvfile = dirpath+'csv/Region3Geo.csv'
        df.to_csv(outcsvfile, encoding='utf-8', header=True, index=False)


# Define directory path, in docker container, to output csv file.
dirpath = '/home/data/ingestProcessing/'
# Create input file name from sys argv input.
infile = sys.argv[1].lower()+'_fort.63_mod.nc'
# Create csv output file.
createCSVFile(dirpath, infile)

