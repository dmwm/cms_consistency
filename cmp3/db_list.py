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
python db_list.py [options] -c <config.yaml> <rse_name>
    -c <YAML config file>       
    -d <CFG config file>       
    -i <states>             -- include only these states
    -x <states>             -- exclude replica states
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

opts, args = getopt.getopt(sys.argv[1:], "c:i:x:d:")
opts = dict(opts)

if not args or ("-c" not in opts and "-d" not in opts):
    print (Usage)
    sys.exit(2)

include_states = opts.get("-i", "*")
exclude_states = opts.get("-x", "")

rse_name = args[0]

if "-c" in opts:
    dbconfig = DBConfig.from_yaml(opts["-c"])
else:
    dbconfig = DBConfig.from_cfg(opts["-d"])

try:
    Base = declarative_base()
    if dbconfig.Schema:
        Base.metadata.schema = dbconfig.Schema

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

    engine = create_engine(dbconfig.DBURL,  echo=verbose)
    Session = sessionmaker(bind=engine)
    session = Session()

    rse = session.query(RSE).filter(RSE.rse == rse_name).first()
    if rse is None:
            print ("RSE %s not found" % (rse_name,))
            sys.exit(1)

    replicas = session.query(Replica).filter(Replica.rse_id==rse.id)
    if include_states != '*':
        replicas = replicas.filter(Replica.state.in_(list(include_states)))
    if exclude_states:
        replicas = replicas.filter(Replica.state.not_in(list(exclude_states)))

    for r in replicas.yield_per(10000):
        path = r.name
        state = r.state
        name = r.name
        scope = r.scope
        print(stats, name, path or "")
