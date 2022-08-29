import sys, time, getopt, json
from stats import Stats
from run import CCRun

def diff(prev, last):
    prev_set = set(prev)
    new_set = set()
    old_set = set()
    for f in last:
        if f in prev_set:
            new_set.add(f)
        else:
            old_set.add(f)
    return old_set, new_set

Usage = """
python diffs.py [options] <storage path> <RSE> [<run id>]
    -p                      - print results to stdout
        -j                  - if -p is used, print results as JSON
    -u                      - update run stats in place
    -s <JSON file>          - save results into JSON stats file
    -S                      - section key for the stats file, used with -s or -u. Default: "diffs"
"""

opts, args = getopt.getopt(sys.argv[1:], "s:S:jpu")
opts = dict(opts)

if not args:
    print(Usage)
    sys.exit(2)

section_key = opts.get("-S", "diffs")

as_json = "-j" in opts

run_id = None
path, rse = args[0], args[1]
if len(args) > 2:
    run_id = args[2]
    run = CCRun(path, rse, run_id)
else:
    run = CCRun.last_run_for_rse(path, rse)

if run is None or not run.is_complete():
    print("Last run not found or incomplete", file=sys.stderr)
    sys.exit(1)
    
prev_run = run.previous_run()
if prev_run is None or not prev_run.is_complete():
    print("Previous run not found or incomplete", file=sys.stderr)
    sys.exit(1)
    
old_dark, new_dark = diff(prev_run.dark_files(), run.dark_files())
old_missing, new_missing = diff(prev_run.missing_files(), run.missing_files())

diff_data = dict(
        run=run.Run,
        prev_run=prev_run.Run,
        nmissing_old=len(old_missing), 
        nmissing_new=len(new_missing), 
        ndark_old=len(old_dark), 
        ndark_new=len(new_dark)
)

if "-p" in opts:
    if as_json:
        print(json.dumps(diff_data, indent=4, sort_keys=True))
    else:
        for k, v in sorted(diff_data.items()):
            print(f"{k}: {v}")

if "-u" in opts:
    stats = Stats(run.stats_path())
    stats.update(section_key, diff_data)

if "-s" in opts:
    stats = Stats(opts["-s"])
    stats.update(section_key, diff_data)


    


    
    