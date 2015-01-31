"""
Ratings table Data Access Object
"""


TABLENAME = 'ratings'

def create(conn):
    """
    Creates a requested table
    :param table: name of the table
    :param conn: open connection to DB
    :return: None
    """
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS {0}(
          userid integer,
          movie integer,
          rating numeric
        );
        """.format(TABLENAME))


def insert(ratings, conn):
    """
    Insert passed ratings into Ratings table
    :param ratings: list of ratings to insert
    :param conn: open connection to DB
    :return:None
    """
    values = []
    with conn.cursor() as cur:
        for rating in ratings:
            values.append(cur.mogrify("(%s,%s,%s)", rating))
        cur.execute('INSERT INTO {0} VALUES '.format(TABLENAME) + ','.join(values))