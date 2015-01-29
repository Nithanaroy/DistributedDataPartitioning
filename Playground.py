#!/usr/bin/python2.7
#
# Small script to show PostgreSQL and Pyscopg together
#
__author__ = 'Nitin Pasumarthy'

import psycopg2


def simple_function():
    """
    This is a simple function which does nothing
    :rtype : int
    """
    print('Hi, I am a simple function')
    return 1


if __name__ == '__main__':
    # simple_function()
    # print('Hello World')
    try:
        conn = psycopg2.connect("dbname='mydb' user='postgres' host='localhost' password='1234'")
        cur = conn.cursor()
        cur.execute("""SELECT * from weather""")
        rows = cur.fetchall()
        print "\nShow me the databases:\n"
        for row in rows:
            print "   ", row
        cur.close()
        conn.close()
    except:
        print "I am unable to connect to the database"
