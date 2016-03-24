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
- Read the most recent line of the admon database of a beegfs file system
- Extract the total performance metrics of the metadata and storage servers
- Post the metrics to a elastic server
TODO list:
- Improve logging
	- Add TimedRotatingFileHandler
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
import logging.handlers
import argparse
from time import sleep


# global variables
_SCRIPT_VERSION = '0.1'
LOG_LEVEL = logging.ERROR  # NOTSET DEBUG INFO WARNING ERROR CRITICAL


# Define class metrics metadata
class MetricsMeta(object):
    '''
    '''
    def __init__(self, workRequests, queuedRequests):
        self.workRequests = workRequests
        self.queuedRequests = queuedRequests

    def sum_metric(self, newmetric):
        self.workRequests += newmetric.workRequests
        self.queuedRequests += newmetric.queuedRequests


# Define class metrics storage
class MetricsStorage(object):
    '''
    '''
    def __init__(self, diskRead, diskWrite, diskReadPerSec, diskWritePerSec, diskSpaceTotal, diskSpaceFree):
        self.diskRead = diskRead
        self.diskWrite = diskWrite
        self.diskReadPerSec = diskReadPerSec
        self.diskWritePerSec = diskWritePerSec
        self.diskSpaceTotal = diskSpaceTotal
        self.diskSpaceFree = diskSpaceFree

    def sum_metric(self, newmetric):
        self.diskRead += newmetric.diskRead
        self.diskWrite += newmetric.diskWrite
        self.diskReadPerSec += newmetric.diskReadPerSec
        self.diskWritePerSec += newmetric.diskWritePerSec
        self.diskSpaceTotal += newmetric.diskSpaceTotal
        self.diskSpaceFree += newmetric.diskSpaceFree


# Obtain the metrics related to the metadata severs and return the results
def select_metrics_meta(con_db, metaname):
    '''
    :return metrics_meta:
    '''
    cursor = con_db.cursor()
    cursor.execute("select workRequests, queuedRequests from metaNormal where nodeID=? order by time desc limit 0,1", (metaname,))
    row = cursor.fetchone()
    metrics_meta = MetricsMeta(row[0], row[1])
    return metrics_meta


#
def metrics_meta_json(metrics_meta):
    '''

    Args:
        metrics:

    Returns:
        body_meta

    '''
    body_meta = {
        "timestamp": datetime.now(),
        "workRequests": metrics_meta.workRequests,
        "queuedRequests": metrics_meta.queuedRequests
    }
    return body_meta


# Obtain the metrics related to the storage severs and return the results
def select_metrics_storage(con_db, stoname):
    '''
    :return body_storage:
    '''
    cursor = con_db.cursor()
    cursor.execute("select diskRead, diskWrite, diskReadPerSec, diskWritePerSec, diskSpaceTotal, diskSpaceFree from storageNormal where nodeID=? order by time desc limit 0,1", (stoname,))
    row = cursor.fetchone()
    metrics_storage = MetricsStorage(row[0], row[1], row[2], row[3], row[4], row[5])
    return metrics_storage


#
def metrics_storage_json(metrics_storage):
    '''
    Args:
        metrics_storage:

    Returns:
        body_storage

    '''
    #Create elastic query body
    body_storage = {
        "timestamp": datetime.now(),
        "diskRead": metrics_storage.diskRead,
        "diskWrite": metrics_storage.diskWrite,
        "diskReadPerSec": metrics_storage.diskReadPerSec,
        "diskWritePerSec": metrics_storage.diskWritePerSec,
        "diskSpaceTotal": metrics_storage.diskSpaceTotal,
        "diskSpaceFree": metrics_storage.diskSpaceFree
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

    # Get args
    args = parseargs()
    # Parse args
    if args.cfgfile is not None:
        cfgfile = r'{0}'.format(args.cfgfile)
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

    # Set log file
    if args.logfile is True:
        log_filename = args.logfile
    else:
        log_filename = "/var/log/sqlite2elastic-" + config.get('general', 'name') + ".log"

    # Set log level
    if args.loglevel is True:
        log_level = args.loglevel
    else:
        log_level = LOG_LEVEL

    # Configure logging
    logging.basicConfig(level=log_level, filename=log_filename, format='%(asctime)s - %(levelname)s - %(message)s')

    # Test connection to elastic server
    try:
        req = requests.get("http://" + config.get('elastic', 'address') + ":" + config.get('elastic', 'port'))
        es = elasticsearch.Elasticsearch([{'host':  config.get('elastic', 'address'), 'port':  config.get('elastic', 'port')}])
    except Exception as ex:
        logger.exception("Elastic server " + config.get('elastic', 'address') + ":" + config.get('elastic', 'port'))
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
        # Select beegfs database metaserver
        list_server = config.options('metadata')
        num_server = len(list_server)
        # Read meta metrics of each server and acumulate
        for cont in range(0, num_server):
            logger.info("Obtaining metrics " + list_server[cont])
            if cont is 0:
                metrics_meta_total = select_metrics_meta(con_db, list_server[cont])
            else:
                metrics_meta_total.sum_metric(select_metrics_meta(con_db, list_server[cont]))
        # Convert to JSON
        body_metrics = metrics_meta_json(metrics_meta_total)
        # POST metrics to elastic
        try:
            res_meta = es.index(index="beegfs-" + config.get('general', 'name') + "-metadata", doc_type="metrics-meta",  body=body_metrics)
        except Exception as ex:
            logger.exception("Exporting to elastic failed")
            failed_state = True

        # Select beegfs database storage
        list_server = config.options('storage')
        num_server = len(list_server)
        # Read storage metrics of each server and acumulate
        for cont in range(0, num_server):
            logger.info("Obtaining metrics " + list_server[cont])
            if cont is 0:
                metrics_storage_total = select_metrics_storage(con_db, list_server[cont])
            else:
                metrics_storage_total.sum_metric(select_metrics_storage(con_db, list_server[cont]))
        # Convert to JSON
        body_metrics = metrics_storage_json(metrics_storage_total)
        # POST metrics to elastic
        try:
            res_storage = es.index(index="beegfs-" + config.get('general', 'name') + "-storage", doc_type="metrics-storage",  body=body_metrics)
        except Exception as ex:
            logger.exception("Exporting to elastic failed")
            failed_state = True
       
        # Check for consecutive failed connections
        if failed_state is True:
            num_failed += 1

        # Reset the counter after 50
        if (num_loops > 50):
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
    parser.add_argument("-f", "--cfgfile", help='Specify the config file (./sqlite2elastic.ini by default)')
    parser.add_argument("-l", "--logfile", help='Specify the log file (/var/log/sqlite2elastic-fsname.log by default)')
    parser.add_argument("-d", "--loglevel", choices=['logging.DEBUG', 'logging.INFO', 'logging.WARNING', 'logging.ERROR', 'logging.CRITICAL'], help='Set the logging level')
    parser.add_argument("-v", "--version", help="show program's version number and exit", action='version', version=_SCRIPT_VERSION)
    return parser.parse_args()


# MAIN
if __name__ == '__main__':
    main()

