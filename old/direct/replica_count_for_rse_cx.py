import cx_Oracle, sys, uuid

from dburl import schema as oracle_schema, user, password, host, port, service

conn = cx_Oracle.connect(user, password, "%s:%s/%s" % (host, port, service))

rse_name = sys.argv[1]

c = conn.cursor()

c.execute("""select id from %(schema)s.rses where rse=:rse_name""" % {"schema":oracle_schema},
	rse_name=rse_name)

rse_id=c.fetchone()[0]

rse_id = uuid.UUID(bytes=bytes(rse_id)).hex.upper()

#c.execute("""select * from %(schema)s.rses where id=:rse_id"""  % {"schema":oracle_schema}, rse_id=rse_id)
#print c.fetchone()
#sys.exit(0)

print type(rse_id), rse_id
#print rse_id.bytes

c.execute("""select count(*)
			from %(schema)s.replicas rep
			where 
				rep.rse_id=:rse_id
		""" % 
		{"schema":oracle_schema},
		rse_id=rse_id
)

print "%s: %d" % (rse_name, c.fetchone()[0])
