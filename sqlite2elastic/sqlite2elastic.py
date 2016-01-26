#!/usr/bin/env python

# compressandmove.py Code
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
Code sample to compress and move folders
Notes:
-
TODO list:
"""


# stdlib imports
import argparse
import os
import tarfile
import shutil
# import errno
# import string


# global variables
_SCRIPT_VERSION = '1.0'

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
