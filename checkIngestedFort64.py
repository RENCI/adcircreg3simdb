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
    stormtable = storm.lower()+'_fort64'

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
    f = open(dirpath+'stats/'+storm.lower()+'_fort64.csv','w')
    f.write('timestamp,minuvel,maxuvel,meanuvel,stduvel,countuvel,minvvel,maxvvel,meanvvel,stdvvel,countvvel\n')

    # Create stormtable name.
    stormtable = storm.lower()+'_fort64'

    try:
        # Open connection to reg3sim database.
        conn = psycopg2.connect("dbname='reg3sim' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Query storm table, in reg3sim database, calculate statistics, and write to output file. 
        for timestamp in timestamps:
            cur.execute("""SELECT MIN(u_vel), MAX(u_vel), AVG(u_vel), STDDEV(u_vel), COUNT(u_vel), MIN(v_vel), MAX(v_vel), AVG(v_vel), STDDEV(v_vel), COUNT(v_vel) FROM %(storm_table)s WHERE timestamp = %(time_stamp)s""",
                       {'storm_table': AsIs(stormtable), 'time_stamp': timestamp})

            statisticsdb = cur.fetchall()
            f.write(timestamp+','+str(statisticsdb[0][0])+','+str(statisticsdb[0][1])+','+str(statisticsdb[0][2])+','+str(statisticsdb[0][3])+','+str(statisticsdb[0][4])+','+str(statisticsdb[0][5])+','+str(statisticsdb[0][6])+','+str(statisticsdb[0][7])+','+str(statisticsdb[0][8])+','+str(statisticsdb[0][9])+'\n')


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    f.close()

# This function produces statistics, at each timestamp, from a storm track netcdf file. 
def getStormNCStats(storm):
    # Create storm table name, define directory path and read netcdf file. 
    stormtable = storm.lower()+'_fort64'
    dirpath = '/home/data/ingestProcessing/'
    ncvel = xr.open_dataset(dirpath+'nc/'+storm.lower()+'_fort.64.nc', drop_variables=['neta', 'nvel'])

    # Open csv file to output 15 and 45 minute timestamps statistics, and write header. 
    # This is a record of skipping those timestamps.
    with open(dirpath+'ingest/'+stormtable+'_1545.csv', 'a') as file:
        file.write('timestamp\n')

    file.close()

    # Get timestamps.
    veltime = ncvel.variables['time'][:].data

    # Open output statistics file, and write header.
    f = open(dirpath+'stats/'+storm.lower()+'_nc_fort64.csv','w')
    f.write('timestamp,minuvel,maxuvel,meanuvel,medianuvel,stduvel,countuvel,nanuvel,minvvel,maxvvel,meanvvel,medianvvel,stdvvel,countvvel,nanvvel\n')

    # Loop through each timestamp in netcdf file.
    for i in range(len(veltime)):
        # Get current timestamps.
        timestamp = str(veltime[i])
        # Get current minute of timestamp.
        dminute = str(veltime[i]).split(':')[1]

        # If dminute is 00 or 30 produce statistics, and out put to statistics file.
        if dminute == '00' or dminute == '30':
            # Produce statistics for the u-vel variable.
            uvel_data = ncvel.variables['u-vel'][i,:].data
            uvelmin = str(np.nanmin(uvel_data))
            uvelmax = str(np.nanmax(uvel_data))
            uvelmean = str(np.nanmean(uvel_data))
            uvelmedian = str(np.nanmedian(uvel_data))
            uvelstd = str(np.nanstd(uvel_data))
            uvelcount = str(len(np.argwhere(~np.isnan(uvel_data))))
            uvelnan = str(len(np.argwhere(np.isnan(uvel_data))))

            # Produce statistics for the v-vel variable.
            vvel_data = ncvel.variables['v-vel'][i,:].data
            vvelmin = str(np.nanmin(vvel_data))
            vvelmax = str(np.nanmax(vvel_data))
            vvelmean = str(np.nanmean(vvel_data))
            vvelmedian = str(np.nanmedian(vvel_data))
            vvelstd = str(np.nanstd(vvel_data))
            vvelcount = str(len(np.argwhere(~np.isnan(vvel_data))))
            vvelnan = str(len(np.argwhere(np.isnan(vvel_data))))

            # Write statistics to the output csv file.
            f.write(timestamp+','+uvelmin+','+uvelmax+','+uvelmean+','+uvelmedian+','+uvelstd+','+uvelcount+','+uvelnan+','+vvelmin+','+vvelmax+','+vvelmean+','+vvelmedian+','+vvelstd+','+vvelcount+','+vvelnan+'\n')

        else:
            # Else output the timestamp to the 15 or 45 minute csv file.
            with open(dirpath+'stats/'+stormtable+'_1545.csv', 'a') as file:
                file.write(str(timestamp)+'\n')

            file.close()

    f.close()

# get storm track from sys argv input.
storm = sys.argv[1]
# Produce statistics from netcdf file.
getStormNCStats(storm)
# Produce statistics from reg3sim database.
getStormPGStats(storm)
