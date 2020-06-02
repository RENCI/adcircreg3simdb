#!/home/data/anaconda3/bin/python
# -*- coding: utf-8 -*-

import psycopg2, glob, sys
from psycopg2.extensions import AsIs

def ingestGeo():
    tablename = 'r3sim_fort_geom'
    indexname = tablename+'_geom_index'
    try:
        conn = psycopg2.connect("dbname='reg3sim' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")
        cur.execute("""CREATE TABLE %(table_name)s(node INTEGER PRIMARY KEY, lon  NUMERIC,
                                      lat NUMERIC, bathymetry NUMERIC)""",
                {'table_name': AsIs(tablename)})

        with open('/home/data/csv/Region3Geo.csv','r') as f:
            next(f) # Skip the header row.
            cur.copy_from(f,tablename,sep=',')

        conn.commit()
        cur.execute("""ALTER TABLE %(table_name)s ADD COLUMN geom geometry(POINT,4326)""",
                {'table_name': AsIs(tablename)})
        cur.execute("""CREATE INDEX %(index_name)s ON %(table_name)s USING SPGIST ( geom )""",
                {'table_name': AsIs(tablename), 'index_name': AsIs(indexname)})
        cur.execute("""UPDATE %(table_name)s SET geom = ST_SetSRID(ST_MakePoint(lon,lat),4326)""",
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
ingestGeo()
