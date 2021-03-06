#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Import modules.
import sys, psycopg2
from psycopg2.extensions import AsIs
import xarray as xr
import pandas as pd
import numpy as np

# This function gets timestamps of the storm tack, from the reg3sim database.
def getTimeStamps(storm):
    # Create storm table name.
    stormtable = storm.lower()+'_swan63'

    try:
        # Open connect to reg3sim database.
        conn = psycopg2.connect("dbname='reg3sim' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Query database for timestamps, and output to list.
        cur.execute("""SELECT DISTINCT timestamp FROM %(storm_table)s ORDER BY timestamp""", 
                   {'storm_table': AsIs(stormtable)})
        timestampsdb = cur.fetchall()
        timestamps = []

        for timestamp in timestampsdb:
            timestamps.append(str(timestamp[0]))

        return timestamps

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

# This function produces statistics, at each timestamp, from a storm track in the reg3sim database.
def getStormPGStats(storm):
    # Get timestamps for storm
    timestamps = getTimeStamps(storm)
    #timestamps = ['2000-09-02 03:00:00', '2000-09-02 10:00:00', '2000-09-01 15:30:00']

    # Define directory path, in container, open output stats file, and write header.
    dirpath = '/home/data/ingestProcessing/'
    f = open(dirpath+'stats/'+storm.lower()+'_swan63.csv','w')
    f.write('timestamp,minhs,maxhs,meanhs,stdhs,counths,mintps,maxtps,meantps,stdtps,counttps,mindir,maxdir,meandir,stddir,countdir\n')

    # Create stormtable name.
    stormtable = storm.lower()+'_swan63'

    try:
        # Open connection to reg3sim database.
        conn = psycopg2.connect("dbname='reg3sim' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Query storm table, in reg3sim database, calculate statistics, and write to output file.
        for timestamp in timestamps:
            cur.execute("""SELECT MIN(hs), MAX(hs), AVG(hs), STDDEV(hs), COUNT(hs), MIN(tps), MAX(tps), AVG(tps), STDDEV(tps), COUNT(tps), MIN(dir), MAX(dir), AVG(dir), STDDEV(dir), COUNT(dir) FROM %(storm_table)s WHERE timestamp = %(time_stamp)s""",
                       {'storm_table': AsIs(stormtable), 'time_stamp': timestamp})

            statisticsdb = cur.fetchall()
            f.write(timestamp+','+str(statisticsdb[0][0])+','+str(statisticsdb[0][1])+','+str(statisticsdb[0][2])+','+str(statisticsdb[0][3])+','+str(statisticsdb[0][4])+','+str(statisticsdb[0][5])+','+str(statisticsdb[0][6])+','+str(statisticsdb[0][7])+','+str(statisticsdb[0][8])+','+str(statisticsdb[0][9])+','+str(statisticsdb[0][10])+','+str(statisticsdb[0][11])+','+str(statisticsdb[0][12])+','+str(statisticsdb[0][13])+','+str(statisticsdb[0][14])+'\n')


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    f.close()

# This function produces statistics, at each timestamp, from a storm track netcdf files.
def getStormNCStats(storm):
    # Create storm table name, define directory path and read netcdf files.
    dirpath = '/home/data/ingestProcessing/'
    nchs = xr.open_dataset(dirpath+'nc/'+storm.lower()+'_swan_HS.63_mod.nc')
    nctps = xr.open_dataset(dirpath+'nc/'+storm.lower()+'_swan_TPS.63_mod.nc')
    ncdir = xr.open_dataset(dirpath+'nc/'+storm.lower()+'_swan_DIR.63_mod.nc')

    # Get timestamps for each of the swan variables.
    hstime = nchs.variables['time'][:].data
    tpstime = nctps.variables['time'][:].data
    dirtime = ncdir.variables['time'][:].data

    # Open csv file to output statistics.
    f = open(dirpath+'stats/'+storm.lower()+'_swan.csv','w')

    # Check length of swan variables with each other.
    f.write('Time HS vs Time TPS is '+str(len(np.setdiff1d(hstime,tpstime)))+'\n')
    f.write('Time HS vs Time DIR is '+str(len(np.setdiff1d(hstime,dirtime)))+'\n')
    f.write('\n')

    # Write header for statistics
    f.write('timestamp,minhs,maxhs,meanhs,medianhs,stdhs,counths,nanhs,mintps,maxtps,meantps,mediantps,stdtps,counttps,nantps,mindir,maxdir,meandir,mediandir,stddir,countdir,nandir\n')

    # Loop through each timestamp in netcdf files.
    for i in range(len(hstime)):
        # Get current timestamps.
        timestamp = str(hstime[i])

        # Produce statistics for the hs variable.
        hs_data = nchs.variables['hs'][i,:].data
        hsmin = str(np.nanmin(hs_data))
        hsmax = str(np.nanmax(hs_data))
        hsmean = str(np.nanmean(hs_data))
        hsmedian = str(np.nanmedian(hs_data))
        hsstd = str(np.nanstd(hs_data))
        hscount = str(len(np.argwhere(~np.isnan(hs_data))))
        hsnan = str(len(np.argwhere(np.isnan(hs_data))))

        # Produce statistics for the tps variable.
        tps_data = nctps.variables['tps'][i,:].data
        tpsmin = str(np.nanmin(tps_data))
        tpsmax = str(np.nanmax(tps_data))
        tpsmean = str(np.nanmean(tps_data))
        tpsmedian = str(np.nanmedian(tps_data))
        tpsstd = str(np.nanstd(tps_data))
        tpscount = str(len(np.argwhere(~np.isnan(tps_data))))
        tpsnan = str(len(np.argwhere(np.isnan(tps_data))))

        # Produce statistics for the dir variable.
        dir_data = ncdir.variables['dir'][i,:].data
        dirmin = str(np.nanmin(dir_data))
        dirmax = str(np.nanmax(dir_data))
        dirmean = str(np.nanmean(dir_data))
        dirmedian = str(np.nanmedian(dir_data))
        dirstd = str(np.nanstd(dir_data))
        dircount = str(len(np.argwhere(~np.isnan(dir_data))))
        dirnan = str(len(np.argwhere(np.isnan(dir_data))))

        # Write statistics to the output csv file.
        f.write(timestamp+','+hsmin+','+hsmax+','+hsmean+','+hsmedian+','+hsstd+','+hscount+','+hsnan+','+tpsmin+','+tpsmax+','+tpsmean+','+tpsmedian+','+tpsstd+','+tpscount+','+tpsnan+','+dirmin+','+dirmax+','+dirmean+','+dirmedian+','+dirstd+','+dircount+','+dirnan+'\n')

    f.close()

# get storm track from sys argv input.
storm = sys.argv[1]
# Produce statistics from netcdf file.
getStormNCStats(storm)
# Produce statistics from reg3sim database.
getStormPGStats(storm)
