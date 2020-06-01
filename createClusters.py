#!/home/data/anaconda3/bin/python
# -*- coding: utf-8 -*-

import psycopg2, glob, sys
from psycopg2.extensions import AsIs

def addgeom2163():
    tablename = 'r3sim_fort_geom'
    indexname = tablename+'_geom2163_index'
    try:
        conn = psycopg2.connect("dbname='postgres' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")
        cur.execute("""SELECT AddGeometryColumn('public', %(table_name)s, 'geom2163', 2163, 'POINT', 2)""",
                {'table_name': tablename})
        cur.execute("""UPDATE public.%(table_name)s SET geom2163 = ST_transform(geom, 2163)""",
                {'table_name': AsIs(tablename)})
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
    tablename = 'r3sim_fort_geom'
    clusternode = 'cluster_node_'+cluster
    try:
        conn = psycopg2.connect("dbname='postgres' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")
        cur.execute("""ALTER TABLE %(table_name)s 
                ADD COLUMN %(cluster_node)s INTEGER""",
                {'table_name': AsIs(tablename), 'cluster_node': AsIs(clusternode)})
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

# Runs the programs.
cluster = sys.argv[1]
#addgeom2163()
createclusters(cluster)
