"""
Global constants which affect the entire assignement (project)
"""
DEBUG = True
DATABASE_QUERIES_DEBUG = False

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