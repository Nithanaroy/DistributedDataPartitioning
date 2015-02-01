"""
Ratings table Data Access Object

Caution: as this is not a class, make sure to pass the same tablename at the time of creation and insertion.
Else use the default tablename always! Don't mix and match by passing a name during creation of table and using
default name while inserting data.
"""

import Globals

TABLENAME = 'ratings'


def create(conn, table=TABLENAME, dropifexists=True):
    """
    Creates Ratings table, with given name
    :param conn: open connection to DB
    :param table: name of the table to create. Default will be 'ratings'
    :param dropifexists: delete the given table if exists
    :return: None
    """
    with conn.cursor() as cur:
        if dropifexists: cur.execute('DROP TABLE IF EXISTS {0}'.format(table))
        cur.execute("""
        CREATE TABLE IF NOT EXISTS {0}(
          id BIGSERIAL PRIMARY KEY,
          userid INTEGER,
          movieid INTEGER,
          rating NUMERIC
        );
        """.format(table))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)


def insert(ratings, conn, table=TABLENAME):
    """
    Insert passed ratings into Ratings table
    :param ratings: list of ratings to insert
    :param conn: open connection to DB
    :param table: name of the table to insert into. Default will be 'ratings'
    :return:None
    """
    values = []
    with conn.cursor() as cur:
        for rating in ratings:
            values.append(cur.mogrify("(%s,%s,%s)", rating))
        cur.execute('INSERT INTO {0} (userid, movieid, rating) VALUES '.format(table) + ','.join(values))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)


def insertwithselect(lowerbound, upperbound, desttable, conn, ratingstable=TABLENAME):
    """
    Inserts data from Master Ratings table to a given table after filtering based on lower and upper bounds
    :param lowerbound: exclusive lower bound for the SELECT statement
    :param upperbound: inclusive upper bound for the SELECT statement
    :param desttable: destination table name to copy data into
    :param conn: open database connection
    :param ratingstable: name of ratings table
    :return:None
    """
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO {0} (userid, movieid, rating) (
          SELECT userid, movieid, rating
          FROM {1}
          WHERE rating > {2} AND rating <= {3}
        );""".format(desttable, ratingstable, lowerbound, upperbound))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)