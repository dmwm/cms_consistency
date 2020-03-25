import cx_Oracle, sys

from dburl import schema as oracle_schema, user, password, host, port, service

conn = cx_Oracle.connect(user, password, "%s:%s/%s" % (host, port, service))

c = conn.cursor()
c.execute("""select rse.rse, count(*)
			from %(schema)s.replicas rep, %(schema)s.rses rse 
			where 
				rep.rse_id=rse.id and rep.state='A'
			group by rse.rse
			order by count(*) desc
		""" % 
		{"schema":oracle_schema}
	)
while True:
	tup = c.fetchone()
	if tup:
		print '\t'.join(map(str, tup))
	else:
		break
