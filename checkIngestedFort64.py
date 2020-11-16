#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, psycopg2
from psycopg2.extensions import AsIs
import xarray as xr
import pandas as pd
import numpy as np

def getTimeStamps(storm):
    stormtable = storm.lower()+'_fort64'

    try:
        conn = psycopg2.connect("dbname='reg3sim' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

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

def getStormPGStats(storm):
    timestamps = getTimeStamps(storm)
    #timestamps = ['2000-09-02 03:00:00', '2000-09-02 10:00:00', '2000-09-01 15:30:00']

    dirpath = '/home/data/ingestProcessing/'
    f = open(dirpath+'stats/'+storm.lower()+'_fort64.csv','w')
    f.write('timestamp,minuvel,maxuvel,meanuvel,stduvel,countuvel,minvvel,maxvvel,meanvvel,stdvvel,countvvel\n')

    stormtable = storm.lower()+'_fort64'

    try:
        conn = psycopg2.connect("dbname='reg3sim' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

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


def getStormNCStats(storm):
    stormtable = storm.lower()+'_fort64'
    dirpath = '/home/data/ingestProcessing/'
    ncvel = xr.open_dataset(dirpath+'nc/'+storm.lower()+'_fort.64.nc', drop_variables=['neta', 'nvel'])

    with open(dirpath+'ingest/'+stormtable+'_1545.csv', 'a') as file:
        file.write('timestamp\n')

    file.close()

    veltime = ncvel.variables['time'][:].data

    f = open(dirpath+'stats/'+storm.lower()+'_nc_fort64.csv','w')

    f.write('timestamp,minuvel,maxuvel,meanuvel,medianuvel,stduvel,countuvel,nanuvel,minvvel,maxvvel,meanvvel,medianvvel,stdvvel,countvvel,nanvvel\n')

    for i in range(len(veltime)):
        timestamp = str(veltime[i])
        dminute = str(veltime[i]).split(':')[1]

        if dminute == '00' or dminute == '30':
            uvel_data = ncvel.variables['u-vel'][i,:].data
            uvelmin = str(np.nanmin(uvel_data))
            uvelmax = str(np.nanmax(uvel_data))
            uvelmean = str(np.nanmean(uvel_data))
            uvelmedian = str(np.nanmedian(uvel_data))
            uvelstd = str(np.nanstd(uvel_data))
            uvelcount = str(len(np.argwhere(~np.isnan(uvel_data))))
            uvelnan = str(len(np.argwhere(np.isnan(uvel_data))))

            vvel_data = ncvel.variables['v-vel'][i,:].data
            vvelmin = str(np.nanmin(vvel_data))
            vvelmax = str(np.nanmax(vvel_data))
            vvelmean = str(np.nanmean(vvel_data))
            vvelmedian = str(np.nanmedian(vvel_data))
            vvelstd = str(np.nanstd(vvel_data))
            vvelcount = str(len(np.argwhere(~np.isnan(vvel_data))))
            vvelnan = str(len(np.argwhere(np.isnan(vvel_data))))

            f.write(timestamp+','+uvelmin+','+uvelmax+','+uvelmean+','+uvelmedian+','+uvelstd+','+uvelcount+','+uvelnan+','+vvelmin+','+vvelmax+','+vvelmean+','+vvelmedian+','+vvelstd+','+vvelcount+','+vvelnan+'\n')

        else:
            with open(dirpath+'stats/'+stormtable+'_1545.csv', 'a') as file:
                file.write(str(timestamp)+'\n')

            file.close()

    f.close()

storm = sys.argv[1]
getStormNCStats(storm)
getStormPGStats(storm)
