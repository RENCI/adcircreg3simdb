#!/home/data/anaconda3/bin/python
# -*- coding: utf-8 -*-

import psycopg2, glob, sys
from psycopg2.extensions import AsIs

def createstormtable(storm, cluster, timeinterval):
    tablename = storm.lower()+'_fort_'+cluster
    try:
        conn = psycopg2.connect("dbname='postgres' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")
        cur.execute("""CREATE TABLE %(table_name)s (
                node INTEGER,
                zeta NUMERIC,
                timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                PRIMARY KEY(timestamp, node)
            )""",
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

def intostormtable(storm, cluster):
    intablename = storm.lower()+'_fort'
    outtablename = storm.lower()+'_fort_'+cluster
    clusternode = 'cluster_node_'+cluster
    try:
        conn = psycopg2.connect("dbname='postgres' user='data' host='localhost' port='5432' password='adcirc'")
        cur = conn.cursor()

        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")
        cur.execute("""INSERT INTO %(out_table_name)s(node, zeta, timestamp) 
                SELECT Z.node AS node, Z.zeta AS zeta, Z.timestamp AS timestamp
                FROM %(in_table_name)s AS Z
                INNER JOIN r3sim_fort_geom AS G ON (Z.node=G.node)
                WHERE G.%(cluster_node)s IS NULL
                UNION
                SELECT MIN(Z.node) AS node, AVG(Z.zeta) AS zeta, Z.timestamp AS timestamp
                FROM %(in_table_name)s AS Z
                INNER JOIN r3sim_fort_geom AS G ON (Z.node=G.node)
                WHERE G.%(cluster_node)s IS NOT NULL
                GROUP BY Z.timestamp, G.%(cluster_node)s
                ORDER BY node""",
            {'in_table_name': AsIs(intablename), 'out_table_name': AsIs(outtablename), 'cluster_node': AsIs(clusternode)})

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

# Runs the programs.
storm = sys.argv[1]
cluster = sys.argv[2]
createstormtable(storm, cluster, "2 hour")
intostormtable(storm, cluster)
