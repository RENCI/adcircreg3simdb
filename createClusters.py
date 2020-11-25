#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Import modules.
import psycopg2, glob, sys
from psycopg2.extensions import AsIs

"""
Add geometry column, with epsg 2163, to the r3sim_fort_geom table. 
The projection epsg 2163 is an equal area projection, which DBSCAN requires.
"""
def addgeom2163():
    # Defind geometry table name, and name of geom2163 index.
    tablename = 'r3sim_fort_geom'
    indexname = tablename+'_geom2163_index'
    try:
        # Open connection to reg3sim database.
        conn = psycopg2.connect("dbname='reg3sim' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")
        # Add geometry column, with epsg 2163, to geometry table.
        cur.execute("""SELECT AddGeometryColumn('public', %(table_name)s, 'geom2163', 2163, 'POINT', 2)""",
                {'table_name': tablename})
        # Convert existing geometry (epsg 4326) to epsg 2163 and input into its own column.
        cur.execute("""UPDATE public.%(table_name)s SET geom2163 = ST_transform(geom, 2163)""",
                {'table_name': AsIs(tablename)})
        # Create index for geom2163 using SPGIST.
        cur.execute("""CREATE INDEX %(index_name)s ON %(table_name)s USING SPGIST ( geom2163 )""",
                {'table_name': AsIs(tablename), 'index_name': AsIs(indexname)})
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

def createclusters(cluster):
    # Defind geometry table name, and cluster column name.
    tablename = 'r3sim_fort_geom'
    clusternode = 'cluster_node_'+cluster
    try:
        # Open connection to reg3sim database.
        conn = psycopg2.connect("dbname='reg3sim' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")
        # Add cluster node column.
        cur.execute("""ALTER TABLE %(table_name)s 
                ADD COLUMN %(cluster_node)s INTEGER""",
                {'table_name': AsIs(tablename), 'cluster_node': AsIs(clusternode)})
        # Calculate clusters, using ST_ClusterDBSCAN, and output to cluster node column. 
        # The cluster_num is distance in meters, where nodes should be clustered.
        cur.execute("""UPDATE %(table_name)s
                SET %(cluster_node)s = sub.%(cluster_node)s
                FROM (SELECT node, ST_ClusterDBSCAN(geom2163, eps := %(cluster_num)s, minPoints := 2)
                OVER () AS %(cluster_node)s
                FROM %(table_name)s) AS sub
                WHERE %(table_name)s.node=sub.node""",
                {'table_name': AsIs(tablename), 'cluster_num': AsIs(cluster) ,'cluster_node': AsIs(clusternode)})
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

"""
Get cluster number from sys argv input. The cluster number is in meters, and is the 
the distance in which nodes should be clustered.
""" 
cluster = sys.argv[1]
# Add epsg 2163 to geometry column.
addgeom2163()
# Create clusters.
createclusters(cluster)
