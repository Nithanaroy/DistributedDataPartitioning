__author__ = 'Nitin Pasumarthy'

import psycopg2
from itertools import islice

import RatingsDAO
import Globals
import math

CHUNK_SIZE = 140  # bytes
MAX_LINES_COUNT_READ = 4  # Maximum number of lines to read into memory
DATABASE_NAME = 'dds_assgn1'
MAX_RATING = 5.0
RANGE_PARTITION_TABLE_PREFIX = 'range_part'
RROBIN_PARTITION_TABLE_PREFIX = 'rrobin_part'

DEBUG = True


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
        Globals.printinfo('A database named "{0}" already exists'.format(dbname))

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


def createrangepartitionandinsert(conn, lower_bound, sno, upper_bound, dropifexists=True):
    """
    Creates a new partition table and calls INSERT method of DAO to insert the data
    :param conn: open connection to DB
    :param lower_bound: lower bound on the rating
    :param sno: table number. As single single table is split into parts
    :param upper_bound: inclusive upper bound on the rating to insert in the new table
    :param dropifexists: drops the table if exists
    :return:None
    """
    partition_tablename = '{0}{1}'.format(RANGE_PARTITION_TABLE_PREFIX, sno)
    RatingsDAO.create(conn, partition_tablename, dropifexists)
    RatingsDAO.insertwithselect(lower_bound, upper_bound, partition_tablename, conn)
    if DEBUG: Globals.printinfo('Partition {2}: saved ratings => ({0}, {1}]'.format(lower_bound, upper_bound, sno))


def rangepartition(n, conn):
    """
    Partitions the ratings table in to the given number of partition using Range based partitioning scheme
    Partitioned table names will be starting from 1. If the number of partitions are N, the range of Rating values,
    0 to MAX_RATING, will be split uniformly into N pieces.
    Eg: N = 5
    Partition 1: all movies with a rating => [0, 1]
    Partition 2: all movies with a rating => (1, 2]
    Partition 3: all movies with a rating => (2, 3]
    Partition 4: all movies with a rating => (3, 4]
    Partition 5: all movies with a rating => (4, 5]
    As shown, movies with zero rating will be placed in the first partition
    :param n: Number of partitions
    :param conn: open connection to DB
    :return:None
    """
    if n <= 0 or not isinstance(n, int): raise AttributeError("Number of partitions should be a positive integer")

    inc = round(float(MAX_RATING) / n, 1)  # precision restricted to 1 decimal as Ratings have 0.5 increments
    lower_bound = 0.0
    upper_bound = lower_bound + inc

    sno = 1
    while upper_bound <= MAX_RATING:
        createrangepartitionandinsert(conn, lower_bound, sno, upper_bound)
        lower_bound += inc
        upper_bound += inc
        sno += 1

    # If number of partitions is not divisible by MAX_RATING, the last partition will be missed due to rounding
    if lower_bound != MAX_RATING:
        createrangepartitionandinsert(conn, lower_bound, sno, MAX_RATING)

    # save the movies with zero rating in the first partition
    createrangepartitionandinsert(conn, -1, 1, 0, False)


def createrobinpartitionandinsert(conn, sno, ids, dropifexists=True):
    """
    Creates a new partition table and calls INSERT method of DAO to insert the data
    :param conn: open connection to DB
    :param sno: table number. As single single table is split into parts
    :param ids: IDs of the ratings to insert
    :param dropifexists: drops the table if exists
    :return:None
    """
    partition_tablename = '{0}{1}'.format(RROBIN_PARTITION_TABLE_PREFIX, sno)
    RatingsDAO.create(conn, partition_tablename, dropifexists)
    RatingsDAO.insertids(conn, ids, partition_tablename)
    if DEBUG: Globals.printinfo('Partition {0}: saved {1} ratings => {2}...'.format(sno, len(ids), ids[0:6]))


def roundrobinpartition(n, conn):
    """
    Partition the ratings table into 'n' pieces in a round robin manner
    Partitions will be zero indexed.
    Eg: N = 3 partitions
    Partition 0: IDs = [0,3,6..]
    Partition 1: IDs = [1,3,7...]
    Partition 2: IDs = [2,4,8...]
    :param n: Number of partitions
    :param conn: open connection to DB
    :return:None
    """
    if n <= 0 or not isinstance(n, int): raise AttributeError("Number of partitions should be a positive integer")

    numberofratings = RatingsDAO.numberofratings(conn)
    # Assumption: IDs are in order from 1 to total number of ratings in Ratings table
    allids = range(1, numberofratings + 1)
    for i in range(0, n):
        ratingids = filter(lambda x: x % n == i, allids)
        createrobinpartitionandinsert(conn, i, ratingids)


def rangeinsert(conn, userid, movieid, rating):
    """
    Insert a new rating into range based partitioned tables
    :param conn: open connection to DB
    :param userid: 1st column of ratings table, User ID
    :param movieid: 2nd column of ratings table, Movie ID
    :param rating: 3rd column of ratings table, Rating
    :return:None
    """
    n = 5
    partitionwidth = float(MAX_RATING) / n
    # to handle cases when rating is 0, max function is used. Will be inserted in first patition
    partitionindex = max(int(math.ceil(rating / partitionwidth)), 1)
    destinationtable = RANGE_PARTITION_TABLE_PREFIX + str(partitionindex)
    RatingsDAO.insert([(userid, movieid, rating)], conn, destinationtable)


def rrobininsert(conn, userid, movieid, rating):
    """
    Insert a new rating into round robin based partitioned tables
    :param conn: open connection to DB
    :param userid: 1st column of ratings table, User ID
    :param movieid: 2nd column of ratings table, Movie ID
    :param rating: 3rd column of ratings table, Rating
    :return:None
    """
    n = 3
    numberofratings = RatingsDAO.numberofratings(conn)
    partitionindex = (numberofratings + 1) % n
    destinationtable = RROBIN_PARTITION_TABLE_PREFIX + str(partitionindex)
    RatingsDAO.insert([(userid, movieid, rating)], conn, destinationtable)
    # also insert into the ratings table as we are using computing partition index based on number of
    # rows in ratings table above
    RatingsDAO.insert([(userid, movieid, rating)], conn)


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
            # rangepartition(5, conn)
            # rangeinsert(conn, 10, 292, 0)
            roundrobinpartition(3, conn)
            rrobininsert(conn, 10, 292, 0)
            rrobininsert(conn, 100, 292, 0)
            rrobininsert(conn, 1000, 292, 0)
    except Exception as detail:
        Globals.printerror(detail)