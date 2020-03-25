import cx_Oracle, sys

from dburl import schema as oracle_schema, user, password, host, port, service

conn = cx_Oracle.connect(user, password, "%s:%s/%s" % (host, port, service))

c = conn.cursor()
c.execute("""select rse.rse, count(*) 
			from %(schema)s.replicas rep, %(schema)s.rses rse 
			where 
				rep.rse_id=rse.id and rep.path is not null
			group by rse.rse""" %
		{"schema":oracle_schema})
while True:
	tup = c.fetchone()
	if tup:
		print tup
	else:
		break
