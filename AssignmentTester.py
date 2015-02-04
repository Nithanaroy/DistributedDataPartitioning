__author__ = 'Nitin Pasumarthy'

DATABASE_NAME = 'dds_assgn1'

import psycopg2
import datetime
import time

import Assignment as MyAssignment


# SETUP Functions


def createdb(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection()
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named "{0}" already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# ##############


# Testers

def testloadratings(ratingstablename, filepath, openconnection):
    MyAssignment.loadratings(ratingstablename, filepath, openconnection)


def testrangepartition(ratingstablename, n, openconnection):
    MyAssignment.rangepartition(ratingstablename, n, openconnection)


def testroundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    MyAssignment.roundrobinpartition(ratingstablename, numberofpartitions, openconnection)


def testroundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    MyAssignment.roundrobininsert(ratingstablename, userid, itemid, rating, openconnection)


def testrangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    MyAssignment.rangeinsert(ratingstablename, userid, itemid, rating, openconnection)


def testdelete(openconnection):
    MyAssignment.deletepartitionsandexit(openconnection)


# Utilities
def handleerror(message):
    print('\nE: {0} {1}'.format(getformattedtime(time.time()), message))


def getformattedtime(srctime):
    return datetime.datetime.fromtimestamp(srctime).strftime('%Y-%m-%d %H:%M:%S')


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    MyAssignment.MetaDataDAO.create(openconnection)


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:
        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        createdb(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            before_test_script_starts_middleware(conn, DATABASE_NAME)

            RatingsTable = 'batting'
            try:
                testloadratings(RatingsTable, 'test_data.dat', conn)
            except Exception as e:
                handleerror(e)

            try:
                testrangepartition(RatingsTable, 5, conn)
            except Exception as e:
                handleerror(e)

            try:
                testroundrobinpartition(RatingsTable, 5, conn)
            except Exception as e:
                handleerror(e)

            try:
                testroundrobininsert(RatingsTable, 4, 1, 3, conn)
            except Exception as e:
                handleerror(e)

            try:
                testrangeinsert(RatingsTable, 1, 2, 3, conn)
            except Exception as e:
                handleerror(e)

            # try:
            # testdelete(conn)
            # except Exception as e:
            # handleerror(e)

            choice = raw_input('Delete all tables? ')
            if choice == '':
                testdelete(conn)

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(conn, DATABASE_NAME)

    except Exception as detail:
        handleerror(detail)