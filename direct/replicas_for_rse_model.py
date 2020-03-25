from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects.oracle import RAW
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.dialects.oracle import RAW, CLOB
from sqlalchemy.dialects.mysql import BINARY
from sqlalchemy.types import TypeDecorator, CHAR, String

#from sqlalchemy import schema

from dburl import dburl, schema as oracle_schema

import sys, uuid

Base = declarative_base()
Base.metadata.schema = oracle_schema

rse_name = sys.argv[1]
out = open(sys.argv[2], "w")

class GUID(TypeDecorator):
    """
    Platform-independent GUID type.

    Uses PostgreSQL's UUID type,
    uses Oracle's RAW type,
    uses MySQL's BINARY type,
    otherwise uses CHAR(32), storing as stringified hex values.

    """
    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        elif dialect.name == 'oracle':
            return dialect.type_descriptor(RAW(16))
        elif dialect.name == 'mysql':
            return dialect.type_descriptor(BINARY(16))
        else:
            return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value).lower()
        elif dialect.name == 'oracle':
            return uuid.UUID(value).bytes
        elif dialect.name == 'mysql':
            return uuid.UUID(value).bytes
        else:
            if not isinstance(value, uuid.UUID):
                return "%.32x" % uuid.UUID(value)
            else:
                # hexstring
                return "%.32x" % value

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'oracle':
            return str(uuid.UUID(bytes=value)).replace('-', '').lower()
        elif dialect.name == 'mysql':
            return str(uuid.UUID(bytes=value)).replace('-', '').lower()
        else:
            return str(uuid.UUID(value)).replace('-', '').lower()



class Replica(Base):
	__tablename__ = "replicas"
	path = Column(String)
	state = Column(String)
	rse_id = Column(GUID(), primary_key=True)
	scope = Column(String, primary_key=True)
	name = Column(String, primary_key=True)


class RSE(Base):
	__tablename__ = "rses"
	id = Column(GUID(), primary_key=True)
	rse = Column(String)


engine = create_engine(dburl,  echo=True)
Session = sessionmaker(bind=engine)
session = Session()

rse = session.query(RSE).filter(RSE.rse == rse_name).first()
rse_id = rse.id
print "RSE:", type(rse_id), len(rse_id), repr(rse_id)

replicas = session.query(Replica).filter(Replica.rse_id==rse_id).yield_per(10000)
n = 0
for r in replicas:
		out.write("%s\t%s\t%s\t%s\t%s\n" % (rse_name, r.scope, r.name, r.path or "null", r.state))
		n += 1
		if n % 1000 == 0:
			print(n)
print(n)
out.close()

