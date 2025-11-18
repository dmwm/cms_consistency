import sys, os, getopt, time, json
from datetime import datetime, timedelta

from rucio_consistency import Stats

from run import CCRun
from config import ActionConfiguration

Version = "1.5"

Usage = """
python declare_missing.py [options] <storage_path> <scope> <rse>
    -d                          - dry run - do not declare to Rucio
    -a <account>                - Rucio account to use
    -o (-|<out file>)           - write missing file list to stdout (-) or to a file
    -L (-|<out file>)           - write lost files list to stdout (-) or to this file
    -s <stats file>             - file to write stats to
    -S <stats key>              - key to store stats under, default: "missing_action"
    -c <config.yaml>|rucio      - load configuration from a YAML file or Rucio
    -v                          - verbose output

    The following will override values read from the configuration:
    -f <ratio>                  - max allowed fraction of missing files to total number of files found by the scanner,
                                  floating point, default = 0.05
    -m <days>                   - max age for the most recent run, integer, default = 1 day
"""

def chunked(lst, chunk_size=1000):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i+chunk_size]

def isReplicaLost(states, rse):
    """
    Checks is the replica is AVAILABLE at any other rse apart from the one where it's missing. 
    """
    available_at_other_rses = {key:value for key,value in states.items() if value=='AVAILABLE' and key != rse}
    return len(available_at_other_rses)<1

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
        "permanently_lost_files": None,
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
        print("Latest run:", latest_run.Run)
        print("Files found by scanner in the latest run:", num_scanned)

        missing_count = my_stats["detected_missing_files"] = my_stats["confirmed_missing_files"] = latest_run.missing_file_count()
        status = "done"
        abort = False
        num_expected = latest_run.expected_file_count()
        if num_expected is None:
            # estimate expected number of files as min of number of file in the before and after db dumps
            n_before = latest_run.dbdump_file_count("before")
            n_after = latest_run.dbdump_file_count("after")
            if n_before is not None:
                num_expected = n_before
                if n_after is not None:
                    num_expected = min(num_expected, n_after)

        if num_scanned == 0:
            aborted_reason = "no files found by the scanner"
            print("No files found by the scanner -- aborting action")
            abort = True
        elif num_expected is None:
            aborted_reason = "can not estimate the number of expected files"
            print("No estimate for the number of expected files -- aborting action")
            abort = True
        else:
            ratio = 0
            if num_expected > 0:
                ratio = missing_count/num_expected
            print("Missing replicas:", missing_count, "  expected:", num_expected, "  ratio:", "%.2f%%" % (ratio*100.0,))
            if ratio > fraction:
                abort = True
                aborted_reason = "too many missing files: %d (%.2f%% > %.2f%%)" % (missing_count, ratio*100.0, fraction*100.0)
                print("Missing ratio is too high (above %.2f%%) -- aborting action" % (fraction*100.0,))

        if abort:
            status = "aborted"
        elif missing_count > 0:
            if out is not None:
                for f in latest_run.missing_files():
                    print(f, file=out)
                if out is not sys.stdout:
                    out.close()                 
            if not dry_run:
                missing_list = [{"scope":scope, "rse":rse, "name":f} for f in latest_run.missing_files()]
                lost_files = []

                try:
                    from rucio.client.replicaclient import ReplicaClient
                    client = ReplicaClient(account=account)
                    not_declared = []
                    # chunk the list to avoid "request too large" errors
                    for chunk in chunked(missing_list):
                        # generates list of replicas across all rses for each chunk
                        replicas = list(client.list_replicas(dids=[{'scope':element['scope'],'name':element['name']} for element in chunk]))
                        # filters the lost replicas
                        lost_replicas = [replica['name'] for replica in replicas if isReplicaLost(replica['states'],rse=rse)]
                        lost_files.extend(lost_replicas)
                        result = client.declare_bad_file_replicas(chunk, "detected missing by CE", force=True)
                        not_declared += result.pop(rse, [])      # there should be no other RSE in there
                        assert not result, "Other RSEs in the not_declared dictionary: "  + ",".join(result.keys())
                except Exception as e:
                    status = "failed"
                    error = f"Rucio declaration error: {e}"
                else:
                    not_declared_count = len(not_declared)
                    if not_declared_count:
                        print("Replicas failed to declare:", not_declared_count)
                    declaration_errors = {}
                    for item in not_declared:
                        words = item.split(None, 1)
                        if len(words) == 2:
                            declaration_errors[error] = declaration_errors.get(words[1], 0) + 1
                    my_stats["declaration_errors"] = declaration_errors
                    my_stats["declared_missing_files"] = len(missing_list) - not_declared_count

                try:
                    my_stats["permanently_lost_files"] = len(lost_files)
                    if outLost is not None:
                        lost_files_to_write = '\n'.join(str(item) for item in lost_files)
                        outLost.write(lost_files_to_write)
                        if outLost is not sys.stdout:
                            outLost.close()
                except Exception as e:
                    status = "failed"
                    error = f"Rucio lost file exporting error: {e}"


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

opts, args = getopt.getopt(sys.argv[1:], "h?o:L:m:f:s:S:c:vda:")
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

outLost = None
if "-L" in opts:
    if opts["-L"] == "-":
        outLost = sys.stdout
    else:
        outLost = open(opts["-L"], "w")

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
