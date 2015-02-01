"""
Global constants which affect the entire assignement (project)
"""
DEBUG = True
DATABASE_QUERIES_DEBUG = False

# Keys strings for meta data table
RANGE_PARTITIONS_KEY = 'rangepartitions'
RROBIN_PARTITIONS_KEY = 'robinpartitions'
RROBIN_LAST_INSERT_PARTITION_KEY = 'robinlastinsertpartitionindex'
# #################

import datetime
import time


def printerror(message):
    print('E: {0} {1}'.format(getformattedtime(time.time()), message))


def printinfo(message):
    print('I: {0} {1}'.format(getformattedtime(time.time()), message))


def printquery(querystring):
    print('Q: {0} {1}'.format(getformattedtime(time.time()), querystring))


def getformattedtime(srctime):
    return datetime.datetime.fromtimestamp(srctime).strftime('%Y-%m-%d %H:%M:%S')