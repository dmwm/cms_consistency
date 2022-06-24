import sys, os, getopt, time
from datetime import datetime, timedelta

from run import CCRun
from cmp3.stats import Stats

Usage = """
python declare_dark.py [options] <storage_path> <rse>
    -f <ratio, floating point>  - max allowed fraction of confirmed dark files to total number of files found by the scanner,
                                  default = 0.05
    -m <days>                   - max age for the most recent run, default = 1 day
    -M <days>                   - max age for oldest run to use for confirmation, default = 14 days
    -n <number>                 - min number of runs to use to produce the confirmed dark list, 
    -o (-|<out file>)           - write confirmed dark list and write it to the file or stdout if "-", instead of sending to Rucio
    -s <stats file>             - file to write stats to
    -S <stats key>              - key to store stats under, default: "dark_action"
"""

def dark_action(storage_dir, rse, max_age_last, max_age_first, min_runs, out, stats, stats_key):
    t0 = time.time()
    my_stats = {
        "elapsed": None,
        "start_time": t0,
        "end_time": None,
        "status": "started",
        "initial_dark_files": None,
        "confirmed_dark_files": None,
        "aborted_reason": None,
        "error": None,
        "runs_compared": None
    }

    if stats is not None:
        stats[stats_key] = my_stats

    runs = CCRun.runs_for_rse(storage_path, rse)
    now = datetime.now()
    recent_runs = sorted(
            [r for r in runs if r.Timestamp >= now - timedelta(days=age_first)], 
            key=lambda r: r.Timestamp
    )

    status = "started"
    aborted_reason = None
    latest_dark_count = None
    confirmed_dark_count = None
    error = None
    
    if recent_runs:
        my_stats["runs_compared"] = [r.Run for r in recent_runs]

    if len(recent_runs) < min_runs:
        status = "aborted"
        print("not enough runs to produce confirmed dark list:", len(recent_runs), "   required:", min_runs, file=sys.stderr)
    
    elif recent_runs[-1].Timestamp < now - timedelta(days=age_last):
        status = "aborted"
        aborted_reason = "latest run too old: %s" % (latest_run.Timestamp,)

    else:
        latest_run = recent_runs[-1]
        num_scanned = latest_run.scanner_num_files()
        print("Latest run:", latest_run.Run, file=sys.stderr)
        print("Files found by scanner in the latest run:", num_scanned, file=sys.stderr)

        confirmed = None
        for run in recent_runs[::-1]:
            if confirmed is None:
                confirmed = set(run.dark_files())
                latest_dark_count = len(confirmed)
            else:
                new_confirmed = set()
                for f in run.dark_files():
                    if f in confirmed:
                        new_confirmed.add(f)
                confirmed = new_confirmed
                
        confirmed = confirmed or set()

        my_stats["confirmed_dark_files"] = confirmed_dark_count = len(confirmed)
        ratio = confirmed_dark_count/num_scanned
        print("Confirmed dark replicas:", confirmed_dark_count, "(%.2f%%)" % (ratio*100.0,), file=sys.stderr)
        
        status = "done"
        if confirmed:
            if ratio > fraction:
                status = "aborted"
                aborted_reason = "too many dark files: %d (%.2f%% > %.2f%%)" % (confirmed_dark_count, ratio*100.0, fraction*100.0)
            else:
                if out is not None:
                    for f in sorted(confirmed):
                        print(f, file=out)
                    if out is not sys.stdout:
                        out.close()                 # yes, paranoia
                else:
                    try:
                        from rucio.client.replicaclient import ReplicaClient
                        client = ReplicaClient()
                        client.quarantine_replicas(confirmed, rse=rse)
                    except Exception as e:
                        error = f"rucio error: {e}"
                        status = "failed"

    t1 = time.time()
    my_stats.update(dict(
        elapsed = t1-t0,
        end_time = t1,
        status = status,
        error = error,
        initial_dark_files = latest_dark_count,
        confirmed_dark_files = confirmed_dark_count,
        aborted_reason = aborted_reason
    ))

    if stats is not None:
        stats[stats_key] = my_stats
    
    return my_stats

if not sys.argv[1:] or sys.argv[1] == "help":
    print(Usage)
    sys.exit(2)

opts, args = getopt.getopt(sys.argv[1:], "h?o:M:m:n:f:s:S:")
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
stats_file = opts.get("-s")
stats = stats_key = None
if stats_file is not None:
    stats = Stats(stats_file)
stats_key = opts.get("-S", "dark_action")

final_stats = dark_action(storage_path, rse, age_last, age_first, min_runs, out, stats, stats_key)

print("Final status:", final_stats["status"])
if final_stats["status"] == "aborted":
    print("Reason:", final_stats["aborted_reason"])

if final_stats["status"] != "done":
    sys.exit(1)






