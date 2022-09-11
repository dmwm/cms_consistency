import getopt, os, time, re, gzip, json, traceback
import sys, uuid

from config import DBConfig, DBDumpConfiguration
from part import PartitionedList

from sqlalchemy import create_engine, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker

from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.dialects.oracle import RAW, CLOB
from sqlalchemy.dialects.mysql import BINARY
from sqlalchemy.types import TypeDecorator, CHAR, String

from stats import Stats

Version = "1.2"

t0 = time.time()

#from sqlalchemy import schema

Usage = """
python update_replica.py [options] <new state> <rse> <scope> <name>
    -c <YAML config file>       
    -d <CFG config file>       
    -t (replicas|bad|quarantined)  -- table to use, default: replicas
    -i <states>             -- include only these states
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
    cache_ok = True
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

opts, args = getopt.getopt(sys.argv[1:], "c:i:d:t:")
opts = dict(opts)

replicas_table = opts.get("-t", "replicas")

if "-c" not in opts and "-d" not in opts:
    print (Usage)
    sys.exit(2)

new_state, rse_name, scope, name = args

if "-c" in opts:
    dbconfig = DBConfig.from_yaml(opts["-c"])
else:
    dbconfig = DBConfig.from_cfg(opts["-d"])

Base = declarative_base()
if dbconfig.Schema:
    Base.metadata.schema = dbconfig.Schema

class Replica(Base):
        __tablename__ = "replicas"
        state = Column(String)
        rse_id = Column(GUID(), primary_key=True)
        scope = Column(String, primary_key=True)
        name = Column(String, primary_key=True)
        reason = Column(String)
        account = Column(String)
        bytes = Column(Integer)



class BadReplica(Base):
        __tablename__ = "bad_replicas"
        state = Column(String)
        rse_id = Column(GUID(), primary_key=True)
        scope = Column(String, primary_key=True)
        name = Column(String, primary_key=True)

class QuarantinedReplica(Base):
    """Represents the quarantined replicas"""
    __tablename__ = 'quarantined_replicas'
    rse_id = Column(GUID(), primary_key=True)
    path = Column(String)
    scope = Column(String, primary_key=True)
    name = Column(String, primary_key=True)

class RSE(Base):
        __tablename__ = "rses"
        id = Column(GUID(), primary_key=True)
        rse = Column(String)

models = {
    "replicas": Replica,
    "bad": BadReplica,
    "quarantined": QuarantinedReplica
}
assert replicas_table in models
model = models[replicas_table]

engine = create_engine(dbconfig.DBURL,  echo="-v" in opts)
Session = sessionmaker(bind=engine)
session = Session()

rse = session.query(RSE).filter(RSE.rse == rse_name).first()
if rse is None:
        print ("RSE %s not found" % (rse_name,))
        sys.exit(1)

#
# get RSE names mapping
#
rse_names = {}
rses = session.query(RSE)
for r in rses:
    rse_names[r.id] = r.rse

initial = session.query(model).filter(model.rse_id==rse.id, model.scope==scope, model.name==name).first()
print("Initial:", initial.state)

if False:
    replicas = session.query(model).filter(model.rse_id==rse.id, model.scope==scope, model.name==name)
    values = {'state': new_state}
    replicas.update(values)
else:
    initial.state = new_state
    initial.save()


updated = session.query(model).filter(model.rse_id==rse.id, model.scope==scope, model.name==name).first()
print("Updated:", updated.state)

session.commit()
