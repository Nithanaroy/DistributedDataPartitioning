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


def numberofratings(conn, table=TABLENAME):
    """
    Computes the number of records in the Ratings table
    :param conn: open connection to DB
    :param table: name of the Ratings table if other than default
    :return:An integer, number of records in the table
    """
    with conn.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM {0};'.format(table))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)
        return cur.fetchone()[0]


def insertids(conn, ids, desttable, ratingstable=TABLENAME):
    """
    Insert the given IDs from ratings table into a partition (desttable) table
    :param conn: open connection to DB
    :param ids: Rating IDs to be inserted into desttable
    :param desttable: destination table into which these ratings have to be inserted
    :param ratingstable: source table from which ratings are to be picked
    :return:None
    """
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO {0} (userid, movieid, rating) (
          SELECT userid, movieid, rating
          FROM {1}
          WHERE id IN ({2})
        );""".format(desttable, ratingstable, ','.join(str(id) for id in ids)))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)


# Assignment 3

def createfromschema(conn, source, dest, dropifexists=True):
    """
    Creates a new table using the schema from source table
    :param conn: open connection to DB
    :param source: table to get the schema from
    :param dest: new table name
    :param dropifexists: drops the dest table if it already exists if set to True
    :return: None
    """
    with conn.cursor() as cur:
        if dropifexists: cur.execute('DROP TABLE IF EXISTS {0}'.format(dest))
        # Clone the table
        cur.execute("""
            CREATE TABLE {0} AS
            TABLE {1};
        """.format(dest, source))
        # Delete the records in the new table
        cur.execute('DELETE FROM {0}'.format(dest))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)


def addcolumn(conn, table, col, type):
    with conn.cursor() as cur:
        cur.execute('alter table {0} add column {1} {2};'.format(table, col, type))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)


def insertwithselectgeneric(selectcol, allcols, lowerbound, upperbound, desttable, conn, tablename=TABLENAME):
    """
    Inserts data from Master Ratings table to a given table after filtering based on lower and upper bounds
    :param lowerbound: exclusive lower bound for the SELECT statement
    :param upperbound: inclusive upper bound for the SELECT statement
    :param desttable: destination table name to copy data into
    :param conn: open database connection
    :param tablename: name of ratings table
    :return:None
    """
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO {0} ({1}) (
          SELECT {1}
          FROM {2}
          WHERE {3} > {4} AND {3} <= {5}
        );""".format(desttable, ','.join(allcols), tablename, selectcol, lowerbound, upperbound))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)


def create2(conn, table=TABLENAME, dropifexists=True):
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
          rating NUMERIC,
          tupleorder NUMERIC
        );
        """.format(table))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)


def insert2(tuples, conn, cols, table=TABLENAME):
    """
    Insert passed tuples into 'table'
    :param tuples: list of tuples to insert
    :param conn: open connection to DB
    :param table: name of the table to insert into. Default will be 'TABLENAME'
    :return:None
    """
    values = []
    with conn.cursor() as cur:
        for tuple in tuples:
            values.append(cur.mogrify("({0})".format(','.join(['%s' for i in range(0, len(cols))])), tuple))
        cur.execute('INSERT INTO {0} ({1}) VALUES '.format(table, ','.join(cols)) + ','.join(values))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)


def get_column_names(conn, table):
    with conn.cursor() as cur:
        cur.execute('select column_name from information_schema.columns where table_name=%s;', (table, ))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)
        ratings_cols = [i[0] for i in cur.fetchall()]
    return ratings_cols


def sort_rows_and_save(conn, col, order, tuple_order_start, sourcetable, desttable):
    """
    get a list of rows from 'sourcetable' sorted by 'col'
    :param conn: open connection to db
    :param col: column to sort on
    :param order: 'ASC' or 'DESC' for ascending or descending order
    :param tuple_order_start: This is the starting value for tuple order column while saving the sorted tuples
    :param sourcetable: name of the sourcetable to get data from
    :param desttable: name of the table to save the sorted data
    :return: None as this method generally runs in a thread
    :throws: ValueError if column is not found
    """
    with conn.cursor() as cur:
        cur.execute('SELECT * FROM {0};'.format(sourcetable))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)
        ratings = cur.fetchall()

        # Get the list of columns in the given table
        ratings_cols = get_column_names(conn, sourcetable)
        index = ratings_cols.index(col)  # find the index of the sort column in the table

        def sort_key(item):
            return item[index]

        res = []
        for t in sorted(ratings, key=sort_key, reverse=(order == 'DESC')):
            res.append(t + (tuple_order_start,))  # TODO: Have to ignore the id column before insert if present
            tuple_order_start += 1

        if Globals.DEBUG: Globals.printinfo(res)

        ratings_cols.append('tupleorder')

        insert2(res, conn, ratings_cols, desttable)


def get_min_max(conn, col, tablename):
    with conn.cursor() as cur:
        cur.execute('SELECT MIN({0}), MAX({2}) FROM {1};'.format(col, tablename, col))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)
        return cur.fetchone()