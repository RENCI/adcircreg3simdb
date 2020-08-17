#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2, sys
from psycopg2.extensions import AsIs

def gettimestamps(storm, cluster):
    if cluster.lower() == 'none':
        tablename = storm.lower()+'_fort63'
    elif cluster.isalpha() == False:
        tablename = storm.lower()+'_fort63_'+cluster
    else:
        sys.exit('Need to provide cluster, either none or a number')

    try:
        conn = psycopg2.connect("dbname='reg3sim' user='data' host='localhost' port='5432' password='adcirc'")
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

def dropview(storm, cluster, timestamp):
    if cluster.lower() == 'none':
        tablename = storm.lower()+'_fort63'
    elif cluster.isalpha() == False:
        tablename = storm.lower()+'_fort63_'+cluster
    else:
        sys.exit('Need to provide cluster, either none or a number')

    viewname = "_".join([tablename, "".join("".join(timestamp.split(':')[0:2]).split('-')[1:3])])
    try:
        conn = psycopg2.connect("dbname='reg3sim' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")
        cur.execute("""DROP VIEW %(view_name)s""",
                       {'view_name': AsIs(viewname)})
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

storm = sys.argv[1]
cluster = sys.argv[2]
timestamps = gettimestamps(storm, cluster)
#timestamp = '2000-09-03T16:00:00'
#print("_".join([tablename, "".join("".join(timestamp.split(':')[0:2]).split('-')[1:3])]))
#dropview(tablename,timestamp)

for timestamp in timestamps:
    timestamp = "T".join(timestamp.strip().split(' '))
    #print(tablename,timestamp)
    dropview(storm, cluster, timestamp)

