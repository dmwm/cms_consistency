import sys, os, getopt
from datetime import timedelta, datetime

from run import CCRun

Usage = """
python missing_action.py [options] <storage_path> <rse> <scope> 
    -m <max age, days>             - Max age of the latest run in days, default: 1
    -o (-|<out file>)              - produce dark list and write it to the file or stdout if "-", 
                                     instead of sending to Rucio
"""

opts, args = getopt.getopt(sys.argv[1:], "h?m:o:")
opts = dict(opts)

if not args or "-h" in opts or "-?" in opts:
    print(Usage)
    sys.exit(2)

storage_path, rse, scope = args

max_age = int(opts.get("-m", 1))
out = None
if "-o" in opts:
    if opts["-o"] == "-":
        out = sys.stdout
    else:
        out = open(opts["-o"], "w")

runs = list(CCRun.runs_for_rse(storage_path, rse))
if not runs:
    print("No runs found for RSE", rse, file=sys.stderr)
    sys.exit(1)

latest_run = runs[-1]
if latest_run.Timestamp < datetime.now() - timedelta(days=max_age):
    print("Latest run is too old:", latest_run.Run, file=sys.stderr)
    sys.exit(1)

print("Latest run found:", latest_run.Run, file=sys.stderr)

if out is not None:
    for f in latest_run.missing_files():
        print(f, file=out)
else:
    from rucio.client.replicaclient import ReplicaClient
    client = ReplicaClient()
    missing_list = [{"scope":scope, "rse":rse, "name":f} for f in latest_run.missing_files()]
    client.declare_bad_replicas(missing_list, "detected missing by CC")
    print("Missing replicas declared:", len(missing_list), file=sys.stderr)
