#!/home/data/anaconda3/bin/python
# -*- coding: utf-8 -*-

import os, wget, sys

ftype = sys.argv[1]

f = open('/home/data/adcircreg3simdb/files-thredds-'+ftype.strip(),'r')
files = f.readlines()
f.close()

os.chdir('/home/data/nc/')

for file in files:
    url = file.strip()
    filename = wget.download(url)
    prefix = "_".join(url.split('/')[7].split('_')[:2]).lower()

    os.rename(filename,'/home/data/nc/'+prefix+'_'+filename)
