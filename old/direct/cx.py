import cx_Oracle, sys

from dburl import schema as oracle_schema, user, password, host, port, service

conn = cx_Oracle.connect(user, password, "%s:%s/%s" % (host, port, service))

c = conn.cursor()
c.execute("""select path, state from %s.replicas rep, %s.rses rse where rep.rse_id=rse.id and rse.rse=:rse""" % (oracle_schema, oracle_schema), 
		rse=sys.argv[1])
while True:
	tup = c.fetchone()
	if tup:
		print tup
	else:
		break
