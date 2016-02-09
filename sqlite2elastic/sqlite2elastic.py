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
- Improve logging
- Evaluate if beegfs parameters (servers) are correct
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
import logging.handlers
import time



# global variables
_SCRIPT_VERSION = '0.6'
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


#
def select_metrics_meta(con_db, metaname):
    '''
    :return body_meta:
    '''
    cursor = con_db.cursor()
    cursor.execute("select is_responding, workRequests, queuedRequests from metaNormal where nodeID=? order by time desc limit 0,1", (metaname,))
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
    cursor.execute("select is_responding, diskRead, diskWrite, diskReadPerSec, diskWritePerSec, diskSpaceTotal, diskSpaceFree from storageNormal where nodeID=? order by time desc limit 0,1", (stoname,))
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
    # Configure logger
    # General purpose logger
    logger = logging.getLogger(__name__)
    # elastic logger
    # logging.getLogger('elasticsearch.trace')
    # Either write to a file or to stdout
    # estracer.addHandler(logging.FileHandler('/tmp/es_tracer.log'))
    # logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    # Use handler in this way
    # es_tracer = logging.getLogger('elasticsearch.trace')
    # es_tracer.setLevel(logging.INFO)
    # es_tracer.addHandler(logging.FileHandler('/tmp/es_trace.log'))

    # Get args
    args = parseargs()
    # Parse args
    if args.cfgfile is True:
        cfgfile = r"{}".format(args.cfgfile)
    else:
        cfgfile = r'sqlite2elastic.ini'

    # Read config file
    config = ConfigParser.RawConfigParser(allow_no_value=True)
    # Check config file
    try:
        config.read(cfgfile)
    except Exception as ex:
        print("Config file " + cfgfile + " not found or not valid")
        sys.exit()

    # Set logging
    if args.logfile is True:
        log_filename = args.logfile
    else:
        log_filename = "/var/log/sqlite2elastic-" + config.get('general', 'name') + ".log"

    if args.debuglevel is True:
        debug_level = args.debuglevel
    else:
        debug_level = LOG_LEVEL

    # Configure logging
    logging.basicConfig(level=debug_level, filename=log_filename, format='%(asctime)s - %(levelname)s - %(message)s')

    # Test connection to elastic server
    try:
        req = requests.get("http://" + config.get('elastic', 'address') + ":" + config.get('elastic', 'port'))
        es = elasticsearch.Elasticsearch([{'host':  config.get('elastic', 'address'), 'port':  config.get('elastic', 'port')}])
    except Exception as ex:
        logger.exception("Elastic server " + config.get('elastic', 'address') + ":" + config.get('elastic', 'port') + " [ERROR]")
        sys.exit()

    logger.debug(req)
    req.close()
    logger.info("Connection to elastic server: " + config.get('elastic', 'address') + ":" + config.get('elastic', 'port') + " [OK]")

    # Test connection to beegfs sqlite database
    try:
        con_db = sqlite3.connect(config.get('general', 'database'))
    except Exception as ex:
        logger.exception("Database " + config.get('general', 'database') + " not found")
        sys.exit()

    logger.info("Connection to beegfs admon sqlite: " + config.get('general', 'database') + " [OK]")

    # Extract data and export to elastic
    num_failed = 0
    num_loops = 0
    while True and (num_failed < 5):
        failed_state = False
        # Select to beegfs database metaserver
        list_server = config.options('metadata')
        num_server = len(list_server)
        for cont in range(0, num_server):
            logger.info("Obtaining metrics " + list_server[cont])
            body_metrics = select_metrics_meta(con_db, list_server[cont])
            # POST metrics to elastic
            try:
                res_meta = es.index(index="beegfs-" + config.get('general', 'name') + "-" + list_server[cont], doc_type="metrics-meta",  body=body_metrics)
            except Exception as ex:
                logger.exception("Exporting to elastic failed")
                failed_state = True

        # Select to beegfs database storageserver
        list_server = config.options('storage')
        num_server = len(list_server)
        for cont in range(0, num_server):
            logger.info("Obtaining metrics " + list_server[cont])
            body_metrics = select_metrics_storage(con_db, list_server[cont])
            # POST metrics to elastic
            try:
                res_storage = es.index(index="beegfs-" + config.get('general', 'name') + "-" + list_server[cont], doc_type="metrics-storage",  body=body_metrics)
            except Exception as ex:
                logger.exception("Exporting to elastic failed")
                failed_state = True

        # Check if consecutive failed connections
        if failed_state is True:
            num_failed += 1

        if (num_loops > 10):
            num_failed = 0
            num_loops = 0

        num_loops += 1
        logger.info("Sleeping " + str(float(config.get('general', 'time'))) + " seconds")
        sleep(float(config.get('general', 'time')))


    # Close database
    con_db.close()


# Parge arguments
def parseargs():  # pragma: no cover
    """Sets up command-line arguments and parser
    Parameters:
    Returns: parser
    Raises:
    """
    parser = argparse.ArgumentParser(description='Metrics BeeGFS-Sqlite to elastic')
    parser.add_argument("-f", "--cfgfile", default="sqlite2elastic.ini", help='Specify the config file (./sqlite2elastic.ini by default)')
    parser.add_argument("-l", "--logfile", help='Specify the log file (/var/log/sqlite2elastic-fsname.log by default)')
    parser.add_argument("-d", "--debuglevel", choices=['logging.DEBUG', 'logging.INFO', 'logging.WARNING', 'logging.ERROR', 'logging.CRITICAL'], help='Set the logging level')
    parser.add_argument("-v", "--version", help="show program's version number and exit", action='version', version=_SCRIPT_VERSION)
    return parser.parse_args()


# MAIN
if __name__ == '__main__':
    main()

