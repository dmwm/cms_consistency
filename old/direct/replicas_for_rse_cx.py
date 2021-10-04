import cx_Oracle, sys

from dburl import schema as oracle_schema, user, password, host, port, service

conn = cx_Oracle.connect(user, password, "%s:%s/%s" % (host, port, service))


c = conn.cursor()
c.execute("""select rse.rse, rep.scope, rep.name, rep.path, rep.state 
			from %(schema)s.replicas rep, %(schema)s.rses rse 
			where 
				rep.rse_id=rse.id and rse.rse=:rse_name
		""" % 
		{"schema":oracle_schema},
		rse_name=sys.argv[1]
)
while True:
	tup = c.fetchone()
	if tup:
		rse, scope, name, path, state = tup
		print "%s\t%s\t%s\t%s\t%s" % (rse, scope, name, path or "null", state)
	else:
		break
