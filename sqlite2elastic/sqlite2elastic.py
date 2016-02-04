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
- Read the last line of the admon database of a beegfs file system
- Extract the performance metrics of the metadata and storage servers
- Post the metrics to elastic server
TODO list:
- Read options from a config file
- Add infinite loop
"""

# imports
import sys
import sqlite3
import elasticsearch
from datetime import datetime
import requests
import ConfigParser
import logging
import argparse
from time import sleep
import logging
import logging.handlers
import time
# import os
# import json
# import errno
# import string


# global variables
_SCRIPT_VERSION = '0.3'

_CFG_FILE = r'/root/sqlite2elastic.cfg'
# _ELASTIC_SERVER_DIR = "elastic.int.cemm.at"
# _ELASTIC_SERVER_PORT = "9200"
_BEEGFS_DB = "/mnt/fhgfsMgmt/admon/fhgfs-admon-data.db"

pid = "/tmp/test.pid"

LOG_FILENAME = "/tmp/sqlite2elastic.log"
LOG_LEVEL = logging.INFO  # DEBUG WARNING


# Main function
def main():
    """
    Parameters:
    Returns:
    Raises:
        ValueError for invalid arguments
    """
    # get args
    # args = parseargs()

    # Read from config file
    config = ConfigParser.RawConfigParser()
    # Check config file
    try:
        config.read(_CFG_FILE)
    except Exception as ex:
        logging.exception("Config file " + _CFG_FILE + " not found")
        sys.exit()

    # Check connection to elastic server
    try:
        req = requests.get("http://" + config.get('elastic', 'address') + ":" + config.get('elastic', 'port'))
    except Exception as ex:
        logging.exception("Elastic server " + config.get('elastic', 'address') + ":" + config.get('elastic', 'port') + " not reachable")
        sys.exit()

    req.close()

    # Connect to beegfs sqlite database
    try:
        con_db = sqlite3.connect(config.get('beegfs', 'db'))
    except Exception as ex:
        logging.exception("Database " + config.get('beegfs', 'db') + " not found")
        sys.exit()

    # Select to beegfs database
    row_meta = con_db.execute("select is_responding, workRequests, queuedRequests from metaNormal order by time desc limit 0,1")
    row_storage = con_db.execute("select is_responding, diskRead, diskWrite, diskReadPerSec, diskWritePerSec, diskSpaceTotal, diskSpaceFree from storageNormal order by time desc limit 0,1")

    # Extract data from select
    for row in row_meta:
        meta_is_responding = row[0]
        meta_workRequests = row[1]
        meta_queuedRequests = row[2]

    for row in row_storage:
        storage_is_responding = row[0]
        storage_diskRead = row[1]
        storage_diskWrite = row[2]
        storage_diskReadPerSec = row[3]
        storage_diskWritePerSec = row[4]
        storage_diskSpaceTotal = row[5]
        storage_diskSpaceFree = row[6]

    # Close database
    con_db.close()

    # Create elastic query body
    meta_body = {
        "timestamp": datetime.now(),
        "is_responding": meta_is_responding,
        "workRequests": meta_workRequests,
        "queuedRequests": meta_queuedRequests
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

    # Connect to elastic server
    es = elasticsearch.Elasticsearch([{'host': "elastic.int.cemm.at", 'port': 9200}])
    # POST metrics to elastic
    res_meta = es.index(index="beegfs-data-ms01", doc_type="metrics-meta",  body=meta_body)
    res_storage = es.index(index="beegfs-data-storage01", doc_type="metrics-storage", body=storage_body)


# MAIN
if __name__ == '__main__':
    main()

