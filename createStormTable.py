#!/home/data/anaconda3/bin/python
# -*- coding: utf-8 -*-

import psycopg2, glob
from psycopg2.extensions import AsIs

def createtable(tablename, timeinterval):
    try:
        conn = psycopg2.connect("dbname='postgres' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")
        cur.execute("""CREATE TABLE %(table_name)s (
                node INTEGER,
                zeta NUMERIC,
                timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL
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

# Runs the programs.
dirpath = "/home/data/nc/"
storms = [f for f in glob.glob(dirpath+"*.nc")]
dirlength = len(dirpath)

for storm in storms:
    tablename = storm[dirlength:].split('.')[0].lower()
    createtable(tablename,"2 hour")

