import cx_Oracle, sys

from dburl import schema as oracle_schema, user, password, host, port, service
conn = cx_Oracle.connect(user, password, "%s:%s/%s" % (host, port, service))

c = conn.cursor()
c.execute("""select count(*)
		from %(schema)s.replicas rep
			where rep.path is not null and state = 'A'
	""" % {"schema":oracle_schema}
	)
non_nulls = c.fetchone()[0]
print "Non-null paths:", non_nulls

c.execute("""select count(*)
		from %(schema)s.replicas rep
		where state = 'A'
	""" % {"schema":oracle_schema}
	)
N = c.fetchone()[0]

print "All replicas:", N


