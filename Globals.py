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
    print('\nE: {0} {1}'.format(getformattedtime(time.time()), message))


def printinfo(message):
    print('\nI: {0} {1}'.format(getformattedtime(time.time()), message))


def printwarning(message):
    print('\nW: {0} {1}'.format(getformattedtime(time.time()), message))


def printquery(querystring):
    print('\nQ: {0} {1}'.format(getformattedtime(time.time()), querystring))


def getformattedtime(srctime):
    return datetime.datetime.fromtimestamp(srctime).strftime('%Y-%m-%d %H:%M:%S')


# Utility functions
def drange(start, stop, step):
    """
    A range function which allows floating step values
    :param start: starting point, inclusive
    :param stop: ending point, exclusive
    :param step: increment
    :return:Iterator
    """
    r = start
    while r < stop:
        yield r
        r += step