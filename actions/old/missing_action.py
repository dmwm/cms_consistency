import sys, os, getopt, time
from datetime import timedelta, datetime

from run import CCRun
from cmp3.stats import Stats

Usage = """
python missing_action.py [options] <storage_path> <rse> <scope> 
    -f <ratio, floating point>  - max allowed fraction of missing files to total number of files found by the scanner,
                                  default = 0.05
    -m <max age, days>          - Max age of the latest run in days, default: 1 day
    -o (-|<out file>)           - produce dark list and write it to the file or stdout if "-", 
                                  instead of sending to Rucio
    -s <stats file>             - file to write stats to
    -S <stats key>              - key to store stats under, default: "cc_dark"
"""

opts, args = getopt.getopt(sys.argv[1:], "h?m:o:s:S:")
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
        
t0 = time.time()
my_stats = {
    "elapsed": None,
    "start_time": t0,
    "end_time": None,
    "status": "started",
    "initial_miss_files": None,
    "confirmed_miss_files": None,
    "aborted_reason": None
}

status = "started"
aborted_reason = None

stats_file = opts.get("-s")
if stats_file is not None:
    stats_file = Stats(stats_file)
    stats_key = opts.get("-S", "cc_dark")
    stats_file[stats_key] = my_stats

fraction = float(opts.get("-f", 0.05))

runs = list(CCRun.runs_for_rse(storage_path, rse))
if not runs:
    print("No runs found for RSE", rse, file=sys.stderr)
    sys.exit(1)

latest_run = runs[-1]
if latest_run.Timestamp < datetime.now() - timedelta(days=max_age):
    print("Latest run is too old:", latest_run.Run, file=sys.stderr)
    sys.exit(1)

print("Latest run found:", latest_run.Run, file=sys.stderr)

num_scanned = latest_run.scanner_num_files()
num_missing = latest_run.missing_file_count()

if num_missing > num_scanned * fraction:
    print("Too many ")




if out is not None:
    for f in latest_run.missing_files():
        print(f, file=out)
else:
    from rucio.client.replicaclient import ReplicaClient
    client = ReplicaClient()
    missing_list = [{"scope":scope, "rse":rse, "name":f} for f in latest_run.missing_files()]
    client.declare_bad_replicas(missing_list, "detected missing by CC")
    print("Missing replicas declared:", len(missing_list), file=sys.stderr)
