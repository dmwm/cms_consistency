import getopt, os, time, re, gzip, json, traceback
import sys, uuid

from config import DBConfig, CCConfiguration, ConfigYAMLBackend
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
python db_dump.py [options] -c <config.yaml> <rse_name>
    -c <config file> -- required
    -d <db config file> -- required - uses rucio.cfg format. Must contain "default" and "schema" under [databse]
    -v -- verbose
    -n <nparts>
    -f <state>:<prefix> -- filter files with given state to the files set with prefix
        state can be either combination of capital letters or "*" 
        can be repeated  ( -f A:/path1 -f CD:/path2 )
        use "*" for state to send all the files to the output set ( -f *:/path )
    -l -- include more columns, otherwise physical path only, automatically on if -a is used
    -z -- produce gzipped output
    -s <stats file> -- write stats into JSON file
       -S <key> -- add dump stats to stats under the key
    -m <N files> -- stop after N files
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

opts, args = getopt.getopt(sys.argv[1:], "f:c:ln:vd:s:S:zm:")

filters = {}
all_states = set()
for opt, val in opts:
    if opt == '-f':
        states, prefix = val.split(':')
        filters[states] = prefix
        all_states |= set(states)

opts = dict(opts)

if not args or (not "-c" in opts and not "-d" in opts):
        print (Usage)
        sys.exit(2)

verbose = "-v" in opts
long_output = "-l" in opts
out_prefix = opts.get("-o")
zout = "-z" in opts
stats_file = opts.get("-s")
stats_key = opts.get("-S", "db_dump")
stop_after = int(opts.get("-m", 0)) or None

rse_name = args[0]

if "-d" in opts:
    dbconfig = DBConfig.from_cfg(opts["-d"])
else:
    dbconfig = DBConfig.from_yaml(opts["-c"])

#print("dbconfig: url:", dbconfig.DBURL, "schema:", dbconfig.Schema)

config = CCConfiguration(ConfigYAMLBackend(opts["-c"]), rse_name)

stats = None if stats_file is None else Stats(stats_file)

if stats:
    stats[stats_key] = {
        "status":"started",
        "version":Version,
        "rse":rse_name,
        "start_time":t0,
        "end_time":None,
        "files":None,
        "elapsed":None,
        "directories":None,
        "exception":[],  
        "ignored_files":0,
        "ignore_list":None
    }
    
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

    if "-n" in opts:
            nparts = int(opts["-n"])
    else:
            nparts = config.NPartitions

    subdir = config.DBDumpPathRoot
    if not subdir.endswith("/"):    subdir = subdir + "/"
    print(f"Filtering files under {subdir} only")

    ignore_list = config.IgnoreList           
    if ignore_list:
        print("Ignore list:")
        for path in ignore_list:
            print(" ", path)

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

    outputs = {
        states:PartitionedList.create(nparts, prefix, zout) for states, prefix in filters.items()
    }

    all_replicas = '*' in all_states

    replicas = session.query(Replica).filter(Replica.rse_id==rse_id).yield_per(batch)

    if all_replicas:
            sys.stderr.write("including all replias\n")
    else:            
            print("including replicas in states:", list(all_states), file=sys.stderr)
            replicas = replicas.filter(Replica.state.in_(list(all_states)))
    dirs = set()
    n = 0
    filter_re = None    #config.dbdump_param(rse, "filter")
    ignored_files = 0
    if filter_re:
        filter_re = re.compile(filter_re)
    for r in replicas:
        path = r.name
        state = r.state

        if not path.startswith(subdir):
                continue

        if filter_re is not None:
            if not filter_re.search(path):
                continue
            
        if any(path.startswith(ignore_prefix) for ignore_prefix in ignore_list):
            ignored_files += 1
            continue
            
        words = path.rsplit("/", 1)
        if len(words) == 1:
                dirp = "/"
        else:
                dirp = words[0]
        dirs.add(dirp)

        for s, out_list in outputs.items():
            if state in s or s == '*':
                if long_output:
                    out_list.add("%s\t%s\t%s\t%s\t%s" % (rse_name, r.scope, r.name, path or "null", r.state))
                else:
                    out_list.add(path or "null")
        n += 1
        if n % batch == 0:
                print(n)
        if stop_after is not None and n >= stop_after:
            print(f"stopped after {stop_after} files", file=sys.stderr)
            break
    for out_list in outputs.values():
        out_list.close()
    sys.stderr.write("Found %d files in %d directories\n" % (n, len(dirs)))
    t1 = time.time()
    t = int(t1 - t0)
    s = t % 60
    m = t // 60
    sys.stderr.write("Elapsed time: %dm%02ds\n" % (m, s))
except:
    lines = traceback.format_exc().split("\n")
    t1 = time.time()
    if stats is not None:
        stats[stats_key].update({
            "status":"failed",
            "end_time":t1,
            "exception":lines,
            "ignore_list":ignore_list
        })
        stats.save()
    raise
else:    
    if stats is not None:
        stats[stats_key].update({
            "status":"done",
            "end_time":t1,
            "files":n,
            "ignored_files":ignored_files,
            "elapsed":t1-t0,
            "directories":len(dirs),
            "ignore_list":ignore_list
        })
        stats.save()


