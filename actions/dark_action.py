import sys, os, getopt
from datetime import datetime, timedelta

from run import CCRun

Usage = """
python dark_action.py [options] <storage_path> <rse>
    -f <ratio, floating point>  - max allowed fraction of confirmed dark files to total number of files found by the scanner,
                                  default = 0.05
    -m <days>                   - max age for the most recent run, default = 1
    -M <days>                   - max age for oldest run to use for confirmation, default = 14
    -n <number>                 - min number of runs to use to produce the confirmed dark list, 
                                  including the most recent run, default = 2
    -o (-|<out file>)           - produce dark list and write it to the file or stdout if "-", instead of sending to Rucio
"""

opts, args = getopt.getopt(sys.argv[1:], "h?o:M:m:n:f:")
opts = dict(opts)

if not args or "-h" in opts or "-?" in opts:
    print(Usage)
    sys.exit(2)

out = None
if "-o" in opts:
    if opts["-o"] == "-":
        out = sys.stdout
    else:
        out = open(opts["-o"], "w")

storage_path, rse = args
age_first = int(opts.get("-M", 14))
age_last = int(opts.get("-m", 1))
fraction = float(opts.get("-f", 0.05))
min_runs = int(opts.get("-n", 2))

runs = CCRun.runs_for_rse(storage_path, rse)
now = datetime.now()
recent_runs = sorted(
        [r for r in runs if r.Timestamp >= now - timedelta(days=age_first)], 
        key=lambda r: r.Timestamp
)
latest_run = recent_runs[-1]

if len(recent_runs) < min_runs:
    print("Not enough runs to produce confirmed dark list:", len(recent_runs), "   required:", min_runs, file=sys.stderr)
    sys.exit(1)

if latest_run.Timestamp < now - timedelta(days=age_last):
    print("Latest run is too old:", latest_run.Timestamp, file=sys.stderr)
    sys.exit(1)

num_scanned = latest_run.scanner_num_files()
print("Latest run:", latest_run.Run, file=sys.stderr)
print("Files found by scanner in the latest run:", num_scanned, file=sys.stderr)

confirmed = None
for run in recent_runs:
    if confirmed is None:
        confirmed = set(run.dark_files())
    else:
        new_confirmed = set()
        for f in run.dark_files():
            if f in confirmed:
                new_confirmed.add(f)
        confirmed = new_confirmed

if len(confirmed) > int(num_scanned * fraction):
    print("Too many dark files found:", len(confirmed), "   threshold:", int(num_scanned * fraction), file=sys.stderr)
    sys.exit(1)

nreplicas = 0
if out is not None:
    for f in sorted(confirmed):
        print(f, file=out)
        nreplicas += 1
    if out is not sys.stdout:
        out.close()                 # yes, paranoia
else:
    from rucio.client.replicaclient import ReplicaClient
    client = ReplicaClient()
    client.quarantine_replicas(confirmed, rse=rse)
    nreplicas = len(confirmed)
    
print("Confirmed dark replicas:", nreplicas, "(%.2f%%)" % (nreplicas/num_scanned*100.0), file=sys.stderr)

