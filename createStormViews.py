#!/home/jmcmanus/anaconda3/envs/surge/bin/python
# -*- coding: utf-8 -*-

import psycopg2, sys
from psycopg2.extensions import AsIs

def gettimestamps(tablename):
    try:
        conn = psycopg2.connect("dbname='postgres' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        cur.execute("""SELECT DISTINCT timestamp FROM %(table_name)s""",
                   {'table_name': AsIs(tablename)})
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

def createview(tablename, timestamp):
    viewname = "_".join([tablename, "".join("".join(timestamp.split(':')[0:2]).split('-')[1:3])])
    try:
        conn = psycopg2.connect("dbname='postgres' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")
        cur.execute("""CREATE VIEW %(view_name)s AS
                       SELECT Z.node, Z.zeta, Z.timestamp, G.bathymetry, G.geom
                       FROM %(table_name)s AS Z
                       INNER JOIN r3sim_fort_geom AS G ON (Z.node=G.node)
                       WHERE Z.timestamp = %(time_stamp)s""",
                       {'table_name': AsIs(tablename), 'view_name': AsIs(viewname), 'time_stamp':timestamp})
        cur.execute("""COMMIT""")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

# Runs the programs.
#infile = 'bp1_dp1r2b1c2h1l1_fort_timestamps.csv'
#f = open(infile,'r')
#timestamps = f.readlines()
#f.close()

tablename = sys.argv[1]
timestamps = gettimestamps(tablename)
#timestamp = '2000-09-03T16:00:00'
#print("_".join([tablename, "".join("".join(timestamp.split(':')[0:2]).split('-')[1:3])]))
#createview(tablename,timestamp)

for timestamp in timestamps:
    timestamp = "T".join(timestamp.strip().split(' '))
    #print(tablename,timestamp)
    createview(tablename,timestamp)
