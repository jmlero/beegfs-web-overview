#!/bin/python

from datetime import datetime
from elasticsearch import Elasticsearch
import sqlite3
import requests
import json


res = requests.get('http://elastic.int.cemm.at:9200')
con = sqlite3.connect("/mnt/fhgfsMgmt/admon/fhgfs-admon-data.db")
es = Elasticsearch([{'host': 'elastic.int.cemm.at', 'port': 9200}])


print(res.content)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
    return d


#con.row_factory = dict_factory

#cur = con.cursor()

cursor = con.execute("select time, is_responding, workRequests, queuedRequests from metaNormal order by time desc limit 0,1")

#one_row = str(cur.fetchone())
#print (one_row.replace("'",'"'))

for row in cursor:
        print row[0]
        print row[1]
        print row[2]
        print row[3]


#while one_row is not None:
#       print (one_row)
#       one_row = cur.fetchone()

con.close()
