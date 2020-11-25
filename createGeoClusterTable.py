#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Import modules.
import psycopg2, glob, sys
from psycopg2.extensions import AsIs

# This function creates geometry table for clustered geometry.
def creategeomtable(cluster):
    # Devine name of cluster geometry table.
    tablename = 'r3sim_fort_geom_'+cluster
    try:
        # Open connection to reg3sim database.
        conn = psycopg2.connect("dbname='reg3sim' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")
        # Create cluster geometry table.
        cur.execute("""CREATE TABLE %(table_name)s (
                node INTEGER PRIMARY KEY,
                lon  NUMERIC,
                lat NUMERIC,
                bathymetry NUMERIC
            )""",
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

# This function insert clustered geometry into cluster geometry table.
def insertgeomtable(cluster):
    # Define input geometry table name, where the clusters have been derined.
    intablename = 'r3sim_fort_geom'
    # Define output geometry table name, where the clustered geometry will be inserted.
    outtablename = 'r3sim_fort_geom_'+cluster
    # Defind name of cluster geometry table index.
    indexname = 'r3sim_fort_geom_'+cluster+'_index'
    try:
        # Open connection to reg3sim database.
        conn = psycopg2.connect("dbname='reg3sim' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")
        # Insert cluster geometry into cluster geometry table. 
        cur.execute("""INSERT INTO %(out_table_name)s(node, lat, lon, bathymetry)
                SELECT node, lat, lon, bathymetry
                   FROM %(in_table_name)s
                   WHERE cluster_node_100 IS NULL
                UNION
                SELECT MIN(node) AS node, 
                   ST_Y(ST_Centroid(ST_Collect(geom))::geometry(Point,4326)) AS lat,
                   ST_X(ST_Centroid(ST_Collect(geom))::geometry(Point,4326)) AS lon,
                   AVG(bathymetry) AS bathymetry
                FROM %(in_table_name)s
                WHERE cluster_node_100 IS NOT NULL
                GROUP BY cluster_node_100
                ORDER BY node""",
            {'in_table_name': AsIs(intablename), 'out_table_name': AsIs(outtablename)})
        cur.execute("""ALTER TABLE %(out_table_name)s ADD COLUMN geom geometry(POINT,4326)""",
            {'out_table_name': AsIs(outtablename)})
        cur.execute("""CREATE INDEX %(index_name)s ON %(out_table_name)s USING SPGIST ( geom )""",
            {'out_table_name': AsIs(outtablename), 'index_name': AsIs(indexname)})
        cur.execute("""UPDATE %(out_table_name)s  SET geom = ST_SetSRID(ST_MakePoint(lon,lat),4326)""",
            {'out_table_name': AsIs(outtablename)})
        cur.execute("""COMMIT""")
        cur.execute("""ANALYZE %(out_table_name)s""",
                {'out_table_name': AsIs(outtablename)})

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

# Get cluster number from sys argv input.
cluster = sys.argv[1]
# Create geometry table to store clustered geometry.
creategeomtable(cluster)
# Insert clustered geometry into cluster geometry table.
insertgeomtable(cluster)
