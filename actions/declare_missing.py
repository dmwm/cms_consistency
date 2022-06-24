import sys, os, getopt, time
from datetime import datetime, timedelta

from run import CCRun
from cmp3.stats import Stats

Usage = """
python declare_missing.py [options] <storage_path> <scope> <rse>
    -f <ratio, floating point>  - max allowed fraction of confirmed missing files to total number of files found by the scanner,
                                  default = 0.05
    -m <days>                   - max age for the most recent run, default = 1 day
    -o (-|<out file>)           - produce confirmed missing list and write it to the file or stdout if "-", instead of sending to Rucio
    -s <stats file>             - file to write stats to
    -S <stats key>              - key to store stats under, default: "missing_action"
"""

def missing_action(storage_dir, rse, scope, max_age_last, out, stats, stats_key):
    
    t0 = time.time()
    my_stats = {
        "elapsed": None,
        "start_time": t0,
        "end_time": None,
        "status": "started",
        "confirmed_miss_files": None,
        "aborted_reason": None,
        "error": None
    }

    if stats is not None:
        stats[stats_key] = my_stats

    now = datetime.now()
    latest_run = list(CCRun.runs_for_rse(storage_path, rse))[-1]

    status = "started"
    aborted_reason = None
    error = None

    if latest_run.Timestamp < now - timedelta(days=age_last):
        status = "aborted"
        aborted_reason = "latest run too old: %s" % (latest_run.Timestamp,)

    else:
        num_scanned = latest_run.scanner_num_files()
        print("Latest run:", latest_run.Run, file=sys.stderr)
        print("Files found by scanner in the latest run:", num_scanned, file=sys.stderr)

        missing_count = my_stats["confirmed_missing_files"] = latest_run.missing_file_count()

        ratio = missing_count/num_scanned
        print("Missing replicas:", missing_count, "(%.2f%%)" % (ratio*100.0,), file=sys.stderr)

        status = "done"

        if ratio > fraction:
            status = "aborted"
            aborted_reason = "too many missing files: %d (%.2f%% > %.2f%%)" % (missing_count, ratio*100.0, fraction*100.0)
        elif missing_count > 0:
            if out is not None:
                for f in latest_run.missing_files():
                    print(f, file=out)
                if out is not sys.stdout:
                    out.close()                 # yes, paranoia
            else:
                try:
                    from rucio.client.replicaclient import ReplicaClient
                    client = ReplicaClient()
                    missing_list = [{"scope":scope, "rse":rse, "name":f} for f in latest_run.missing_files()]
                    client.declare_bad_replicas(missing_list, "detected missing by CC")
                except Exception as e:
                    status = "failed"
                    error = "Rucio declaration error: {e}"

    t1 = time.time()
    my_stats.update(dict(
        elapsed = t1-t0,
        end_time = t1,
        status = status,
        aborted_reason = aborted_reason,
        error = error
    ))

    if stats is not None:
        stats[stats_key] = my_stats
    
    return my_stats

if not sys.argv[1:] or sys.argv[1] == "help":
    print(Usage)
    sys.exit(2)

opts, args = getopt.getopt(sys.argv[1:], "h?o:m:f:s:S:")
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

storage_path, scope, rse = args
age_last = int(opts.get("-m", 1))
fraction = float(opts.get("-f", 0.05))
stats_file = opts.get("-s")
stats = None
if stats_file is not None:
    stats = Stats(stats_file)
stats_key = opts.get("-S", "missing_action")

final_stats = missing_action(storage_path, rse, scope, age_last, out, stats, stats_key)

print("Final status:", final_stats["status"])
if final_stats["status"] == "aborted":
    print("Reason:", final_stats["aborted_reason"])

if final_stats["status"] != "done":
    sys.exit(1)






