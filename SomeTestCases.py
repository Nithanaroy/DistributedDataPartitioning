import Assignment as a
import psycopg2

c = a.getopenconnection(user='postgres', password='1234', dbname='dds_assgn1')
c.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

t = 'simple'
f = 'test_data.dat'

a.loadratings(t, f, c)

a.rangepartition(t, 5, c)
a.rangeinsert(t, 2, 300, 4, c)
a.deletepartitions(t, c)

a.roundrobinpartition(t, 3, c)
a.roundrobininsert(t, 2, 200, 5, c)
a.deletepartitions(t, c)

# below should raise exception
a.roundrobininsert(t, 2, 200, 5, c)