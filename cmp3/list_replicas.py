import getopt, os, time, re, gzip, json, traceback
import sys, uuid

from config import DBConfig, DBDumpConfiguration
from part import PartitionedList

from sqlalchemy import create_engine
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
python db_list.py [options]
    -c <YAML config file>       
    -d <CFG config file>       
    -r <RSE>
    -n <name>[,...]
    -t (replicas|bad_replicas)  -- table to use, default: replicas
    -i <states>             -- include only these states
    -x <states>             -- exclude replica states
    -s                      -- include scope
    -S                      -- include state
    -v                      -- verbose
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

opts, args = getopt.getopt(sys.argv[1:], "c:i:x:d:vsSr:n:t:")
opts = dict(opts)

if "-c" not in opts and "-d" not in opts:
    print (Usage)
    sys.exit(2)

include_states = opts.get("-i", "*")
exclude_states = opts.get("-x", "")
include_scope = "-s" in opts
include_state = "-S" in opts
rse_name = opts.get("-r")
names = opts.get("-n")
replicas_table = opts.get("-t", "replicas")

assert replicas_table in ("replicas", "bad_replicas")

if "-c" in opts:
    dbconfig = DBConfig.from_yaml(opts["-c"])
else:
    dbconfig = DBConfig.from_cfg(opts["-d"])

Base = declarative_base()
if dbconfig.Schema:
    Base.metadata.schema = dbconfig.Schema

class Replica(Base):
        __tablename__ = replicas_table
        state = Column(String)
        rse_id = Column(GUID(), primary_key=True)
        scope = Column(String, primary_key=True)
        name = Column(String, primary_key=True)

class RSE(Base):
        __tablename__ = "rses"
        id = Column(GUID(), primary_key=True)
        rse = Column(String)

engine = create_engine(dbconfig.DBURL,  echo="-v" in opts)
Session = sessionmaker(bind=engine)
session = Session()

rse = None
if rse_name is not None:
    rse = session.query(RSE).filter(RSE.rse == rse_name).first()
    if rse is None:
            print ("RSE %s not found" % (rse_name,))
            sys.exit(1)

replicas = session.query(Replica)
if rse is not None:
    replicas = replicas.filter(Replica.rse_id==rse.id)

if include_states != '*':
    replicas = replicas.filter(Replica.state.in_(list(include_states)))

if exclude_states:
    replicas = replicas.filter(Replica.state.not_in(list(exclude_states)))
    
if names is not None:
    names = names.split(',')
    replicas = replicas.filter(Replica.name.in_(names))

for r in replicas.yield_per(10000):
    path = r.name
    state = r.state
    name = r.name
    scope = r.scope
    tup = (name,)
    if include_scope:
        tup = (scope,) + tup
    if include_state:
        tup = (state,) + tup
    if include_path:
        tup = tup + (path or "",)
    print(*tup)
