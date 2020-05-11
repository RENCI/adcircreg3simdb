#!/home/data/anaconda3/bin/python
# -*- coding: utf-8 -*-

import os, time, glob
from zipfile import ZipFile

def ingestData(dirpath,inzip):
    if len([f for f in glob.glob("/home/data/ingest")]) == 0:
        os.mkdir("/home/data/ingest")

    tablename = inzip.split('.')[0].lower()

    with open('/home/data/ingest/'+tablename+'.csv', 'a') as file:
        file.write('records_ingested,time_lapsed\n')

    file.close()

    zipfort = ZipFile(dirpath+inzip, 'r')
    ziplist = zipfort.namelist()

    for zipfile in ziplist:
        start_time = time.time()
        zipfort.extract(zipfile,path=dirpath)

        stream = os.popen('timescaledb-parallel-copy --db-name postgres --connection "host=localhost user=data password=adcirc sslmode=disable" --table '+tablename+' --file '+dirpath+zipfile+' --skip-header --workers 4 --copy-options "CSV"')
        output = stream.read()

        os.remove(dirpath+zipfile)
        os.rmdir(dirpath+'csvfort')

        stop_time = time.time()
        time_lapsed = stop_time - start_time

        with open('/home/data/ingest/'+tablename+'.csv', 'a') as file:
            file.write(output.strip()+','+str(time_lapsed)+'\n')

        file.close()

    zipfort.close()

dirpath = "/home/data/zip/"
infiles = [f for f in glob.glob(dirpath+"*.zip")]
infiles.sort()
dirlength = len(dirpath)

for infile in infiles:
    inzip = infile[dirlength:] 
    ingestData(dirpath,inzip)

