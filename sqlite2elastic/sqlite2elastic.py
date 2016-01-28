#!/usr/bin/env python

# sqlite2elastic.py Code
#
# Copyright (c) Jose M. Molero
#
# All rights reserved.
#
# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

"""
Notes:
-
TODO list:
-
"""

# imports
import sqlite3
import elasticsearch
from datetime import datetime
import requests
# import argparse
# import os
# import tarfile
# import shutil
# import json
# import errno
# import string


# global variables
_SCRIPT_VERSION = '0.2'

_ELASTIC_SERVER = 'http://elastic.int.cemm.at:9200'
_ELASTIC_SERVER_PORT = 9200
_BEEGFS_DB = "/mnt/fhgfsMgmt/admon/fhgfs-admon-data.db"


res = requests.get(_ELASTIC_SERVER)
con = sqlite3.connect(_BEEGFS_DB)
es = elasticsearch.Elasticsearch([{'host': 'elastic.int.cemm.at', 'port': _ELASTIC_SERVER_PORT}])

print(res.content)

row_meta = con.execute("select is_responding, workRequests, queuedRequests from metaNormal order by time desc limit 0,1")
row_storage = con.execute("select is_responding, diskRead, diskWrite, diskReadPerSec, diskWritePerSec, diskSpaceTotal, diskSpaceFree from storageNormal order by time desc limit 0,1")

for row in row_meta:
        print row[0]
        print row[1]
        print row[2]
        meta_is_responding = row[0]
        meta_workRequests = row[1]
        meta_queuedRequests = row[2]

for row in row_storage:
        print row[0]
        print row[1]
        print row[2]
        print row[3]
        print row[4]
        print row[5]
        print row[6]
        storage_is_responding = row[0]
        storage_diskRead = row[1]
        storage_diskWrite = row[2]
        storage_diskReadPerSec = row[3]
        storage_diskWritePerSec = row[4]
        storage_diskSpaceTotal = row[5]
        storage_diskSpaceFree = row[6]

meta_body = {
    "timestamp": datetime.now(),
    'is_responding': meta_is_responding,
    'workRequests': meta_workRequests,
    'queuedRequests': meta_queuedRequests
}

storage_body = {
    "timestamp": datetime.now(),
    "is_responding": storage_is_responding,
    "diskRead": storage_diskRead,
    "diskWrite": storage_diskWrite,
    "diskReadPerSec": storage_diskReadPerSec,
    "diskWritePerSec": storage_diskWritePerSec,
    "diskSpaceTotal": storage_diskSpaceTotal,
    "diskSpaceFree": storage_diskSpaceFree
}

print datetime.now()

res_meta = es.index(index="beegfs-data-ms01", doc_type="metrics-meta",  body=meta_body)

res_storage = es.index(index="beegfs-data-storage01", doc_type="metrics-storage", body=storage_body)


res.close()
con.close()
