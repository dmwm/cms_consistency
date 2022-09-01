import sys, os, getopt, time
from datetime import datetime, timedelta

from run import CCRun
from stats import Stats
from config import ActionConfiguration

Version = "1.2"

Usage = """
python declare_dark.py [options] <storage_path> <rse>
    -a <account>                - Rucio account to use
    -o (-|<out file>)           - write confirmed dark list and write it to the file or stdout if "-", instead of sending to Rucio
    -s <stats file>             - file to write stats to
    -S <stats key>              - key to store stats under, default: "dark_action"
    -c <config.yaml>|rucio      - load configuration from a YAML file or Rucio
    -v                          - verbose output

    The following will override values read from the configuration:
    -f <ratio, floating point>  - max allowed fraction of confirmed dark files to total number of files found by the scanner,
                                  default = 0.05
    -w <days>                   - max age for oldest run to use for confirmation, default = 35 days
    -m <days>                   - max age for the most recent run, default = 1 day
    -M <days>                   - min age for oldest run, default = 25
    -n <number>                 - min number of runs to use to produce the confirmed dark list, default = 3
"""

def dark_action(storage_dir, rse, out, stats, stats_key):
    t0 = time.time()
    my_stats = {
        "version": Version,
        "elapsed": None,
        "start_time": t0,
        "end_time": None,
        "status": "started",
        "detected_dark_files": None,
        "confirmed_dark_files": None,
        "aborted_reason": None,
        "error": None,
        "runs_compared": None,
        "configuration": {
            "confirmation_window": window,
            "min_age_first_run": min_age_first,
            "max_age_last_run": max_age_last,
            "min_runs": min_runs,
            "max_fraction": fraction
        }
    }

    if stats is not None:
        stats.update_section(stats_key, my_stats)

    runs = CCRun.runs_for_rse(storage_path, rse)
    now = datetime.now()
    recent_runs = sorted(
            [r for r in runs if r.Timestamp >= now - timedelta(days=window)], 
            key=lambda r: r.Timestamp
    )

    status = "started"
    aborted_reason = None
    latest_dark_count = None
    confirmed_dark_count = None
    detected_dark_count = None
    error = None
    
    if recent_runs:
        my_stats["runs_compared"] = [r.Run for r in recent_runs]

    if not recent_runs or len(recent_runs) < min_runs:
        status = "aborted"
        aborted_reason = "not enough runs to produce confirmed dark list: %d, required: %d" % (len(recent_runs), min_runs)
    else:
        first_run = recent_runs[0]
        latest_run = recent_runs[-1]
        num_scanned = latest_run.scanner_num_files()
        detected_dark_count = latest_run.dark_file_count()
        print("Runs in the confirmation history:", len(recent_runs))
        print("First run:", first_run.Run, file=sys.stderr)
        print("Last run:", latest_run.Run, file=sys.stderr)
        print("  Files in RSE:", num_scanned, file=sys.stderr)
        print("  Dark files:", detected_dark_count, file=sys.stderr)

        if latest_run.Timestamp < now - timedelta(days=max_age_last):
            status = "aborted"
            aborted_reason = "latest run is too old: %s, required: < %d days old" % (latest_run.Timestamp, max_age_last)

        elif first_run.Timestamp > now - timedelta(days=min_age_first):
            status = "aborted"
            aborted_reason = "oldest run is not old enough: %s, required: > %d days old" % (latest_run.Timestamp, min_age_first)

        else:
            confirmed = set(recent_runs[0].dark_files())
            for run in recent_runs[1:]:
                confirmed &= set(run.dark_files())

            confirmed_dark_count = len(confirmed)
            ratio = confirmed_dark_count/num_scanned
            print("Confirmed dark files:", confirmed_dark_count, "(%.2f%%)" % (ratio*100.0,), file=sys.stderr)
        
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
    my_stats.update(
        elapsed = t1-t0,
        end_time = t1,
        status = status,
        error = error,
        detected_dark_files = detected_dark_count,
        confirmed_dark_files = confirmed_dark_count,
        aborted_reason = aborted_reason
    )

    if stats is not None:
        stats.update_section(stats_key, my_stats)
    
    return my_stats

if not sys.argv[1:] or sys.argv[1] == "help":
    print(Usage)
    sys.exit(2)

opts, args = getopt.getopt(sys.argv[1:], "h?o:M:m:w:n:f:s:S:c:v")
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

config = {}
if "-c" in opts:
    config = ActionConfiguration(rse, opts["-c"], "dark")

window = int(opts.get("-w", config.get("confirmation_window", 35)))
min_age_first = int(opts.get("-M", config.get("min_age_first_run", 25)))
max_age_last = int(opts.get("-m", config.get("max_age_last_run", 1)))
fraction = float(opts.get("-f", config.get("max_fraction", 0.05)))
min_runs = int(opts.get("-n", config.get("min_runs", 3)))

stats_file = opts.get("-s")
stats = stats_key = None
if stats_file is not None:
    stats = Stats(stats_file)
stats_key = opts.get("-S", "dark_action")

if "-v" in opts:
    print("\nParameters:")
    print("  stats file:                  ", stats_file)
    print("  stats key:                   ", stats_key)
    print("  config:                      ", opts.get("-c"))
    print("  confirmation window:         ", window)
    print("  min age for last run:        ", min_age_first)
    print("  max age for first run:       ", max_age_last)
    print("  min number of runs:          ", min_runs)
    print("  max dark files fraction:     ", fraction)
    print()

run_stats = dark_action(storage_path, rse, out, stats, stats_key)
status = run_stats["status"]
error = run_stats.get("error")
aborted_reason = run_stats.get("aborted_reason")

if "-v" in opts:
    print("\nFinal stats:")
    for k, v in sorted(run_stats.items()):
        print("%s = %s" % (k, v))
    print()

print("Final status:", status, file=sys.stderr)
if status == "aborted":
    print("  Reason:", aborted_reason, file=sys.stderr)
elif status != "done":
    print("  Error:", error, file=sys.stderr)

if status != "done":
    sys.exit(1)

