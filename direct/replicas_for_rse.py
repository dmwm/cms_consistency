from rucio.client.rseclient import RSEClient
import json, re, getopt, os
import sys, uuid

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

Usage = """
python replicas_for_rse.py -c <config.json> [-o <output file>] <rse_name>
"""


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



class DBConfig:

	def __init__(self, cfg):
		self.Host = cfg["host"]
		self.Port = cfg["port"]
		self.Schema = cfg["schema"]
		self.User = cfg["user"]
		self.Password = cfg["password"]
		self.Service = cfg["service"]

	def dburl(self):
		return "oracle+cx_oracle://%s:%s@%s:%s/?service_name=%s" % (
			self.User, self.Password, self.Host, self.Port, self.Service)
class Config:
	def __init__(self, cfg_file_path):
		cfg = json.load(open(cfg_file_path, "r"))
		self.DBConfig = DBConfig(cfg["database"])
		self.DBSchema = self.DBConfig.Schema
		self.DBURL = self.DBConfig.dburl()
		self.RSEs = cfg["rses"]
		my_name = os.environ.get("USER")
		rucio_cfg = cfg.get("rucio", {})
		self.RucioAccount = rucio_cfg.get("account",my_name)
			

	def lfn_to_pfn(self, rse_name):
		rules = self.RSEs.get(rse_name, self.RSEs.get("*", {}))["lfn_to_pfn"]
		return [ {
			"path":re.compile(r["path"]),
			"out":r["out"].replace("$", "\\")
			} for r in rules
		]

Base = declarative_base()
opts, args = getopt.getopt(sys.argv[1:], "o:c:")
opts = dict(opts)

if not args or not "-c" in opts:
	print Usage
	sys.exit(2)


out = sys.stdout
if "-o" in opts:
	out = open(opts["-o"], "w")
config = Config(opts["-c"])
Base.metadata.schema = config.DBSchema


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

rse_name = args[0]

engine = create_engine(config.DBURL,  echo=True)
Session = sessionmaker(bind=engine)
session = Session()

rse = session.query(RSE).filter(RSE.rse == rse_name).first()
if rse is None:
	print "RSE %s not found" % (rse_name,)
	sys.exit(1)

rse_id = rse.id

print "rse_id:", type(rse_id), rse_id

#
# lfn-to-pfn
#
rules = config.lfn_to_pfn(rse_name)

batch = 100000

replicas = session.query(Replica).filter(Replica.rse_id==rse_id).yield_per(batch)
n = 0
for r in replicas:
		path = r.path
		if not path:
			for rule in rules:
				match = rule["path"]
				rewrite = rule["out"]
				if match.match(r.name):
					path = match.sub(rewrite, r.name)
					break
		out.write("%s\t%s\t%s\t%s\t%s\n" % (rse_name, r.scope, r.name, path or "null", r.state))
		n += 1
		if n % batch == 0:
			print(n)
print(n)
out.close()

