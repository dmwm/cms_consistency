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
python requests.py <dbconfig> <command> [options] <args>

    list 
        -d <RSE>
        -a <activity>
        -s <state>
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

cfg, command, rest = sys.argv[1], sys.argv[2], sys.argv[3:]
dbconfig = DBConfig.from_cfg(cfg)

Base = declarative_base()
if dbconfig.Schema:
    Base.metadata.schema = dbconfig.Schema

class Request(Base):
    __tablename__ = "requests"
    id = Column(GUID(), primary_key=True)
    request_type = Column(String)
    scope = Column(String)
    name = Column(String)
    dest_rse_id = Column(GUID())
    source_rse_id = Column(GUID())
    attributes = Column(String(4000))
    state = Column(Enum(RequestState, name='REQUESTS_STATE_CHK',
                        create_constraint=True,
                        values_callable=lambda obj: [e.value for e in obj]),
                   default=RequestState.QUEUED)
    activity = Column(String(50), default='default')

class RSE(Base):
        __tablename__ = "rses"
        id = Column(GUID(), primary_key=True)
        rse = Column(String)

engine = create_engine(dbconfig.DBURL,  echo="-v" in opts)
Session = sessionmaker(bind=engine)
session = Session()

rses = session.query(RSE).all()
rse_map = {}
for rse in rses:
    print(rse.rse, rse.id)
    rse_map[rse.rse] = rse.id

def do_list(session, argv):

    opts, args = getopt.getopt(argv, "d:a:s:")
    opts = dict(opts)
    
    activity = opts.get("-a")
    dest_rse = opts.get("-d")
    state = opts.get("-s")
    
    
    requests = session.query(Request)
    if activity:    requests = requests.filter(Request.activity == activity)
    if dest_rse:    requests = requests.filter(Request.dest_rse_id == rse_map[dest_rse])
    if state:       requests = requests.filter(Request.state == state)
    
    for request in requests.yield_per(10000):
        print(reqiest.id, request.request_type, request.state, request.activity, request.name)

if command == "list":
    do_list(session, rest)