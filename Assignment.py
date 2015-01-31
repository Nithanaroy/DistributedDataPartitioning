__author__ = 'Nitin Pasumarthy'

import psycopg2
from itertools import islice

import RatingsDAO

CHUNK_SIZE = 140  # bytes
MAX_LINES_COUNT_READ = 4  # Maximum number of lines to read into memory
DATABASE_NAME = 'dds_assgn1'


def readfilebyline(filepath):
    """
    A simple file read function
    :param filepath: relative or abs path of the fle to read
    :return:None
    """
    with open(filepath) as ratings_file:
        for line in ratings_file:
            print(line)


def manualchunkread(filepath):
    """
    Reads a file in chunks as defined by CHUNK_SIZE variable
    :param filepath: relative or abs path of file to read
    :return:None
    """
    f = open(filepath, 'r')
    while True:
        data = f.read(CHUNK_SIZE)
        if not data:
            break
        lines = data.split('\n')
        traceback_amount = len(lines[-1])
        f.seek(-traceback_amount, 1)
        for line in lines[0:-1]:
            print(line)
            print()
        print
    f.close()


def getnextchunk(filepath):
    """
    Reads files in chunks in an efficient manner using isslice method.
    Uses yield to return the next chunk if an existing file is being read
    :param filepath: relative or abs path of the file to read
    :return:Chunk of lines using yield
    """
    with open(filepath) as f:
        while True:
            lines = list(islice(f, MAX_LINES_COUNT_READ))
            if len(lines) < MAX_LINES_COUNT_READ:
                break
            yield lines


def getconnection(dbname='postgres'):
    """
    Connects to dds_assgn1 database using postgres user
    :return: Open DB connection
    """
    return psycopg2.connect("dbname='" + dbname + "' user='postgres' host='localhost' password='1234'")


def createdb(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getconnection()
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


def loadratings(filepath, conn):
    """
    Loads the file into DB
    :param filepath: relative or abs path of the file to load
    :param conn: open connection to DB
    :return: None
    """
    RatingsDAO.create(conn)
    for lines in getnextchunk(filepath):
        ratings = []
        for line in lines:
            rating = line.split('::')[0:3]
            ratings.append(rating)
        RatingsDAO.insert(ratings, conn)



if __name__ == '__main__':
    try:
        # conn = psycopg2.connect("dbname='mydb' user='postgres' host='localhost' password='1234'")
        # cur = conn.cursor()
        # cur.execute("""SELECT * from weather""")
        # rows = cur.fetchall()
        # print "\nShow me the databases:\n"
        # for row in rows:
        # print "   ", row
        # cur.close()
        # conn.close()

        createdb(DATABASE_NAME)

        with getconnection(DATABASE_NAME) as conn:
            loadratings('test_data.dat', conn)

    except Exception as detail:
        print "Error: ", detail