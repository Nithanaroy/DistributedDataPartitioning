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


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    :return:None
    """
    con = psycopg2.connect(user='postgres', host='localhost', password='1234')

    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))
    cur.close()
    con.close()

if __name__ == '__main__':
    # simple_function()
    # print('Hello World')
    try:
        # conn = psycopg2.connect("dbname='mydb' user='postgres' host='localhost' password='1234'")
        # cur = conn.cursor()
        # cur.execute("""SELECT * from weather""")
        # rows = cur.fetchall()
        # print "\nShow me the databases:\n"
        # for row in rows:
        #     print "   ", row
        # cur.close()
        # conn.close()

        create_db('dds_assgn1')
    except Exception as detail:
        print "I am unable to connect to the database", detail
