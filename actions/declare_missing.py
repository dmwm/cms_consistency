import sys, os, getopt, time, json
from datetime import datetime, timedelta

from run import CCRun
from stats import Stats
from config import ActionConfiguration

Version = "1.2"

Usage = """
python declare_missing.py [options] <storage_path> <scope> <rse>
    -d                          - dry run - do not declare to Rucio
    -a <account>                - Rucio account to use
    -o (-|<out file>)           - write missing file list to stdout (-) or to a file
    -s <stats file>             - file to write stats to
    -S <stats key>              - key to store stats under, default: "missing_action"
    -c <config.yaml>|rucio      - load configuration from a YAML file or Rucio
    -v                          - verbose output

    The following will override values read from the configuration:
    -f <ratio>                  - max allowed fraction of missing files to total number of files found by the scanner,
                                  floating point, default = 0.05
    -m <days>                   - max age for the most recent run, integer, default = 1 day
"""

def missing_action(storage_dir, rse, scope, max_age_last, out, stats, stats_key, account, dry_run):
    
    t0 = time.time()
    my_stats = {
        "version": Version,
        "rucio_account": account,
        "dry_run": dry_run,
        "elapsed": None,
        "start_time": t0,
        "end_time": None,
        "status": "started",
        "detected_missing_files": None,
        "confirmed_missing_files": None,
        "declared_missing_files": None,
        "aborted_reason": None,
        "error": None,
        "declaration_errors": {},
        "configuration": {
            "max_age_last_run": age_last,
            "max_fraction": fraction
        }
    }

    if stats is not None:
        stats.update_section(stats_key, my_stats)

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

        missing_count = my_stats["detected_missing_files"] = my_stats["confirmed_missing_files"] = latest_run.missing_file_count()

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
                    out.close()                 
            if not dry_run:
                missing_list = [{"scope":scope, "rse":rse, "name":f} for f in latest_run.missing_files()]

                try:
                    from rucio.client.replicaclient import ReplicaClient
                    client = ReplicaClient(account=account)
                    not_declared = client.declare_bad_file_replicas(missing_list, "detected missing by CC")
                    not_declared = not_declared.pop("rse", [])      # there shuld be no other RSE in there
                    assert not not_declared, "Other RSEs in the not_declared dictionary:"  + str(list(not_declared.keys()))
                except Exception as e:
                    status = "failed"
                    error = f"Rucio declaration error: {e}"

                not_declared_count = len(not_declared)
                if not_declared_count:
                    print("Replicas failed to declare:", not_declared_count)
                declaration_errors = {}
                for lst in not_declared:
                    for item in lst:
                        words = item.split(None, 1)
                        if len(words) == 2:
                            error = words[1]
                            declaration_errors[error] = declaration_errors.get(error, 0) + 1
                my_stats["declaration_errors"] = declaration_errors
                my_stats["declared_missing_files"] = len(missing_list) - not_declared_count

    t1 = time.time()
    my_stats.update(
        elapsed = t1-t0,
        end_time = t1,
        status = status,
        aborted_reason = aborted_reason,
        error = error
    )

    if stats is not None:
        stats.update_section(stats_key, my_stats)

    return my_stats

if not sys.argv[1:] or sys.argv[1] == "help":
    print(Usage)
    sys.exit(2)

opts, args = getopt.getopt(sys.argv[1:], "h?o:m:f:s:S:c:vda:")
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

config = {}
if "-c" in opts:
    config = ActionConfiguration(rse, opts["-c"], "missing")

age_last = int(opts.get("-m", config.get("max_age_last_run", 1)))
fraction = float(opts.get("-f", config.get("max_fraction", 0.01)))
account = opts.get("-a")
dry_run = "-d" in opts

if dry_run:
    print("====== dry run mode ======")


stats_file = opts.get("-s")
stats = None
if stats_file is not None:
    stats = Stats(stats_file)
stats_key = opts.get("-S", "missing_action")

if "-v" in opts:
    print("\nParameters:")
    print("  dry run:                     ", dry_run)
    print("  stats file:                  ", stats_file)
    print("  stats key:                   ", stats_key)
    print("  config:                      ", opts.get("-c"))
    print("  max age for last run:        ", age_last)
    print("  max missing files fraction:  ", fraction)
    print()

final_stats = missing_action(storage_path, rse, scope, age_last, out, stats, stats_key, account, dry_run)

print("Final status:", final_stats["status"])
if final_stats["status"] == "aborted":
    print("  Reason:", final_stats.get("aborted_reason", ""))
elif final_stats["status"] != "done":
    print("  Error:", final_stats.get("error", ""))

if "-v" in opts:
    print("\nFinal stats:")
    for k, v in sorted(final_stats.items()):
        print(f"  {k}:\t{v}")
    print()

if final_stats["status"] != "done":
    sys.exit(1)