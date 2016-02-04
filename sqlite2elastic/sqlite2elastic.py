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
- Logging
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
_SCRIPT_VERSION = '0.4'
LOG_FILENAME = "/tmp/sqlite2elastic.log"
LOG_LEVEL = logging.DEBUG  # NOTSET DEBUG INFO WARNING ERROR CRITICAL


# Define class metrics metadata
class MetricsMeta(object):
    '''
    '''
    def __init__(self, is_responding, workRequests, queuedRequests):
        self.is_responding = is_responding
        self.workRequests = workRequests
        self.queuedRequests = queuedRequests

# Define class metrics storage
class MetricsStorage(object):
    '''
    '''
    def __init__(self, is_responding, diskRead, diskWrite, diskReadPerSec, diskWritePerSec, diskSpaceTotal, diskSpaceFree):
        self.is_responding = is_responding
        self.diskRead = diskRead
        self.diskWrite = diskWrite
        self.diskReadPerSec = diskReadPerSec
        self.diskWritePerSec = diskWritePerSec
        self.diskSpaceTotal = diskSpaceTotal
        self.diskSpaceFree = diskSpaceFree


# Parge arguments
def parseargs():  # pragma: no cover
    """Sets up command-line arguments and parser
    Parameters:
    Returns: parser
    Raises:
    """
    parser = argparse.ArgumentParser(description='Metrics BeeGFS-Sqlite to elastic')
    parser.add_argument("--cfgFile", help='specify the config file (./sqlite2elastic.cfg by default)')
    parser.add_argument("-v", "--version", help="show program's version number and exit", action='version', version=_SCRIPT_VERSION)
    return parser.parse_args()


#
def select_metrics_meta(con_db, metaname):
    '''
    :return body_meta:
    '''
    cursor = con_db.cursor()
    cursor.execute("select is_responding, workRequests, queuedRequests from metaNormal where nodeID=? order by time desc limit 0,1", metaname)
    row = cursor.fetchone()
    metrics_meta = MetricsMeta(row[0], row[1], row[2])

    if ( (metrics_meta.is_responding and metrics_meta.queuedRequests and metrics_meta.workRequests) >= 0 ):
        # Create elastic query body
        body_meta = {
            "timestamp": datetime.now(),
            "is_responding": metrics_meta.is_responding,
            "workRequests": metrics_meta.workRequests,
            "queuedRequests": metrics_meta.queuedRequests
        }
    else:
        # Error
        logging.error("Meta metrics negative ")
        body_meta = {
            "timestamp": datetime.now(),
            "is_responding": 0,
            "workRequests": 0,
            "queuedRequests": 0
        }
    return body_meta


#
def select_metrics_storage(con_db, stoname):
    '''
    :return body_storage:
    '''
    cursor = con_db.cursor()
    cursor.execute("select is_responding, diskRead, diskWrite, diskReadPerSec, diskWritePerSec, diskSpaceTotal, diskSpaceFree from storageNormal where nodeID=? order by time desc limit 0,1", stoname)
    row = cursor.fetchone()
    metrics_storage = MetricsStorage(row[0], row[1], row[2], row[3], row[4], row[5], row[6])

    if ( (metrics_storage.is_responding and metrics_storage.diskRead and metrics_storage.diskWrite and metrics_storage.diskReadPerSec and metrics_storage.diskWritePerSec and metrics_storage.diskSpaceTotal and metrics_storage.diskSpaceFree) >= 0 ):
        #Create elastic query body
        body_storage = {
            "timestamp": datetime.now(),
            "is_responding": metrics_storage.is_responding,
            "diskRead": metrics_storage.diskRead,
            "diskWrite": metrics_storage.diskWrite,
            "diskReadPerSec": metrics_storage.diskReadPerSec,
            "diskWritePerSec": metrics_storage.diskWritePerSec,
            "diskSpaceTotal": metrics_storage.diskSpaceTotal,
            "diskSpaceFree": metrics_storage.diskSpaceFree
        }
    else:
         # Error
        logging.error("Storage metrics negative ")
        body_storage = {
            "timestamp": datetime.now(),
            "is_responding": 0,
            "diskRead": 0,
            "diskWrite": 0,
            "diskReadPerSec": 0,
            "diskWritePerSec": 0,
            "diskSpaceTotal": 0,
            "diskSpaceFree": 0
        }
    return body_storage


# Main function
def main():
    """
    Parameters:
    Returns:
    Raises:
        ValueError for invalid arguments
    """
    # Set logging
    logging.basicConfig(level=LOG_LEVEL)
    logger = logging.getLogger(__name__)

    # Get args
    args = parseargs()
    # Parse args
    if args.cfgFile is True:
        cfgFile = r"{}".format(args.cfgFile)
    else:
        cfgFile = r'sqlite2elastic.cfg'

    # Read config file
    config = ConfigParser.RawConfigParser()
    # Check config file
    try:
        config.read(cfgFile)
    except Exception as ex:
        logger.exception("Config file " + cfgFile + " not found")
        sys.exit()

    logger.info("Reading config file " + cfgFile)

    # Connect to elastic server
    try:
        req = requests.get("http://" + config.get('elastic', 'address') + ":" + config.get('elastic', 'port'))
        es = elasticsearch.Elasticsearch([{'host':  config.get('elastic', 'address'), 'port':  config.get('elastic', 'port')}])
    except Exception as ex:
        logger.exception("Elastic server " + config.get('elastic', 'address') + ":" + config.get('elastic', 'port') + " [ERROR]")
        sys.exit()

    logger.debug(req)
    req.close()
    logger.info("Connection to elastic server: " + config.get('elastic', 'address') + ":" + config.get('elastic', 'port') + " [OK]")

    # Connect to beegfs sqlite database
    try:
        con_db = sqlite3.connect(config.get('beegfs', 'db'))
    except Exception as ex:
        logger.exception("Database " + config.get('beegfs', 'db') + " not found")
        sys.exit()

    logger.info("Connection to beegfs admon sqlite: " + config.get('beegfs', 'db') + " [OK]")

    # Select to beegfs database
    metaname = (config.get('beegfs', 'metadata'),)
    body_meta = select_metrics_meta(con_db, metaname)
    # POST metrics to elastic
    res_meta = es.index(index="beegfs-data-" + config.get('beegfs', 'metadata'), doc_type="metrics-meta",  body=body_meta)

    # Select to beegfs database
    stoname = (config.get('beegfs', 'storage'),)
    body_storage = select_metrics_storage(config, stoname)
    # POST metrics to elastic
    res_storage = es.index(index="beegfs-data-" + config.get('beegfs', 'storage'), doc_type="metrics-storage", body=body_storage)

     # Close database
    con_db.close()


# MAIN
if __name__ == '__main__':
    main()

