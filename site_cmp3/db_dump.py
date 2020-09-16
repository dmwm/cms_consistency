from __future__ import print_function
import getopt, os, time, re
import sys, uuid

from config import DBConfig, Config
from partition import part

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



t0 = time.time()

#from sqlalchemy import schema

Usage = """
python db_dump.py [-a] [-l] [-o<output file>] [-r <path>] -c <config.yaml> <rse_name>
    -c <config file> -- required
    -d <db config file> -- required - uses rucio.cfg format. Must contain "default" and "schema" under [databse]
    -v -- verbose
    -n <nparts>
    -o <prefix> -- output file prefix
    -a -- include all replicas, otherwise active only (state='A')
    -l -- include more columns, otherwise physical path only, automatically on if -a is used
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

opts, args = getopt.getopt(sys.argv[1:], "o:c:lan:vd:")
opts = dict(opts)

verbose = "-v" in opts
all_replicas = "-a" in opts
long_output = "-l" in opts or all_replicas
out_prefix = opts.get("-o")
if not args or (not "-c" in opts and not "-d" in opts):
        print (Usage)
        sys.exit(2)

rse_name = args[0]

if "-d" in opts:
    dbconfig = DBConfig.from_cfg(opts["-d"])
else:
    dbconfig = DBConfig.from_yaml(opts["-c"])

print("dbconfig: url:", dbconfig.DBURL, "schema:", dbconfig.Schema)

config = Config(opts["-c"])

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

if "-n" in opts:
        nparts = int(opts["-n"])
else:
        nparts = config.nparts(rse_name) or 1

if nparts > 1:
        if out_prefix is None:
                print("Output file path must be specified if partitioning is requested")
                sys.exit(1)

outputs = [sys.stdout]
if out_prefix is not None:
        outputs = [open("%s.%05d" % (out_prefix, i), "w") for i in range(nparts)]

subdir = config.dbdump_root(rse_name) or "/"
if not subdir.endswith("/"):    subdir = subdir + "/"

engine = create_engine(dbconfig.DBURL,  echo=verbose)
Session = sessionmaker(bind=engine)
session = Session()

rse = session.query(RSE).filter(RSE.rse == rse_name).first()
if rse is None:
        print ("RSE %s not found" % (rse_name,))
        sys.exit(1)

rse_id = rse.id

#print ("rse_id:", type(rse_id), rse_id)

batch = 100000

if all_replicas:
        sys.stderr.write("including all replias\n")
        replicas = session.query(Replica).filter(Replica.rse_id==rse_id).yield_per(batch)
else:
        sys.stderr.write("including active replias only\n")
        replicas = session.query(Replica)       \
                .filter(Replica.rse_id==rse_id) \
                .filter(Replica.state=='A')     \
                .yield_per(batch)
dirs = set()
n = 0
filter_re = config.dbdump_param(rse, "filter")
if filter_re:
    filter_re = re.compile(filter_re)
for r in replicas:
                path = r.name

                if not path.startswith(subdir):
                        continue

                if filter_re is not None:
                    if not filter_re.search(path):
                        continue

                words = path.rsplit("/", 1)
                if len(words) == 1:
                        dirp = "/"
                else:
                        dirp = words[0]
                dirs.add(dirp)

                ipart = part(nparts, path)
                out = outputs[ipart]

                if long_output:
                        out.write("%s\t%s\t%s\t%s\t%s\n" % (rse_name, r.scope, r.name, path or "null", r.state))
                else:
                        out.write("%s\n" % (path or "null", ))
                n += 1
                if n % batch == 0:
                        print(n)
[out.close() for out in outputs]
sys.stderr.write("Found %d files in %d directories\n" % (n, len(dirs)))
t = int(time.time() - t0)
s = t % 60
m = t // 60
sys.stderr.write("Elapsed time: %dm%02ds\n" % (m, s))

