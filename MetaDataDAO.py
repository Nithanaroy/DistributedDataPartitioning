"""
Has the MetaData about the partitioning
"""

import Globals

TABLENAME = 'patitionmeta'


def create(conn):
    """
    Create a MetaData table if it does not exist
    :param conn: open connection to DB
    :return:None
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS {0}(
              KEY VARCHAR(50),
              VALUE VARCHAR(50)
            )
        """.format(TABLENAME))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)


def upsert(conn, key, value):
    """
    Inserts a given (key, value) pair into meta data table if not present, else updates the value of the key
    :param conn: open connection to DB
    :param key: Key to insert / update
    :param value: Value to insert / update
    :return:None
    """
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM {0} WHERE KEY = '{1}'".format(TABLENAME, key))
        keyvalue = cur.fetchone()
        if keyvalue is None:
            cur.execute("INSERT INTO {0} VALUES ('{1}', '{2}')".format(TABLENAME, key, value))
            if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)
        else:
            cur.execute("UPDATE {0} SET VALUE = '{1}' WHERE KEY = '{2}'".format(TABLENAME, value, key))
            if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)


def select(conn, key):
    """
    Fetches the value of a given key from meta data table
    :param conn: open connection to DB
    :param key: Key to fetch
    :return:value of key if present, else None
    """
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM {0} WHERE KEY = '{1}'".format(TABLENAME, key))
        if Globals.DEBUG and Globals.DATABASE_QUERIES_DEBUG: Globals.printquery(cur.query)
        keyvalue = cur.fetchone()
        if keyvalue is not None: return keyvalue[0]
        return None


def drop(conn):
    """
    Drops the table
    :param conn: open connection to DB
    :return:None
    """
    with conn.cursor() as cur:
        cur.execute('drop table if exists {0};'.format(TABLENAME))