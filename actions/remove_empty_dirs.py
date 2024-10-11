import sys, os, getopt, time, os.path
from datetime import datetime, timedelta
from pythreader import TaskQueue, Task, Primitive, synchronized

from run import CCRun, FileNotFoundException
from config import ActionConfiguration
from rucio_consistency import CEConfiguration, Stats
from rucio_consistency.xrootd import XRootDClient


Version = "1.1"

Usage = """
python remove_empty_dirs.py [options] (<storage_path>|<file path>) <rse>
    -d                          - dry run
    -o (-|<out file>)           - write confirmed empty directory list to stdout (-) or to a file
    -s <stats file>             - file to write stats to
    -S <stats key>              - key to store stats under, default: "empty_action"
    -c <config.yaml>|rucio      - load configuration from a YAML file or Rucio
    -v                          - verbose output

    The following will override values read from the configuration:
    -L <number>                 - stop after removing so many directories
    -w <days>                   - max age for oldest run to use for confirmation, default = 35 days
    -m <days>                   - max age for the most recent run, default = 1 day
    -M <days>                   - min age for oldest run, default = 25
    -n <number>                 - min number of runs to use to produce the confirmed empty directory list, default = 3
"""

class LFNConverter(object):

    def __init__(self, site_root, remove_prefix, add_prefix):
        self.SiteRoot = site_root
        self.RemovePrefix = remove_prefix
        self.AddPrefix = add_prefix

    def canonic(self, path):
        while path and "//" in path:
            path = path.replace("//", "/")
        return path

    def path_to_lfn(self, path):
        assert path.startswith(self.SiteRoot)
        path = self.canonic("/" + path[len(self.SiteRoot):])
        if self.RemovePrefix and path.startswith(self.RemovePrefix):
            path = self.canonic("/" + path[len(self.RemovePrefix):])
        if self.AddPrefix:
            path = self.canonic(self.AddPrefix + "/" + path)
        return path

    def lfn_to_path(self, lfn):
        if self.AddPrefix:
            assert lfn.startswith(self.AddPrefix)
            lfn = lfn[len(self.AddPrefix):]
        path = lfn
        if self.RemovePrefix:
            path = self.RemovePrefix + path
        return self.canonic(self.SiteRoot + "/" + path)

    def lfn_or_path_to_path(self, lfn_or_path):
        if lfn_or_path.startswith(self.SiteRoot):
            return lfn_or_path         # already a path
        return self.lfn_to_path(lfn_or_path)

class RemoveDirectoryTask(Task):

    RETRIES = 3

    def __init__(self, client, path):
        Task.__init__(self)
        self.Client = client
        self.Path = path
        self.Retries = self.RETRIES

    def run(self):
        return self.Client.rmdir(self.Path)


class Remover(Primitive):

    def __init__(self, client, paths, dry_run, limit=None, max_workers=10, verbose=False):
        Primitive.__init__(self)
        self.Client = client
        self.Paths = paths
        self.Queue = TaskQueue(max_workers, capacity=max_workers, stagger=0.1, delegate=self)
        self.Failed = []            # [(path, error), ...]
        self.ErrorCounts = {}
        self.RemovedCount = 0
        self.SubmittedCount = 0
        self.Verbose = verbose
        self.DryRun = dry_run
        self.Limit = limit          # max number of dirs to delete

    def shave(self, paths):
        # split the list of paths (assumed to be reversely ordered) into leaves and inner nodes
        leaves = []
        inner = []
        last_path = None
        for path in paths:
            if last_path is None or not last_path.startswith(path):
                leaves.append(path)
            else:
                inner.append(path)
            last_path = path
        return leaves, inner

    def run(self):
        paths = sorted(self.Paths, reverse=True)
        while paths and (self.Limit is None or self.SubmittedCount < self.Limit):
            leaves, inner = self.shave(paths)
            for leaf in leaves:
                depth = len([p for p in leaf.split('/') if p])      # do not remove root directories like "/store/mc"
                if depth <= 2:
                    print(f"skipping root:", leaf)
                    continue
                if self.Verbose:
                    print(f"submitting (dry_run={self.DryRun}):", leaf)
                if not self.DryRun:
                    self.Queue.append(RemoveDirectoryTask(self.Client, leaf))
                self.SubmittedCount += 1
                if self.Limit is not None and self.SubmittedCount >= self.Limit:
                    print(f"Limit of {self.Limit} reached")
                    break
            if self.Verbose:
                print("waiting for the queue to be empty...")
            self.Queue.waitUntilEmpty()
            paths = inner
        return self.Failed

    @synchronized
    def taskEnded(self, queue, task, result):
        status, error = result
        error = (error or "").strip()
        if self.Verbose:
            print("taskEnded:", task.Path, status, error or "")
        if status != "OK":
            if error == "timeout" and task.Retries > 0:
                task.Retries -= 1
                if self.Verbose:
                    print("resubmitting after timeout:", task.Path)
                self.Queue.append(task)
            else:
                reduced_error = error
                while task.Path in reduced_error:
                    reduced_error = reduced_error.replace(task.Path, "[path]")
                self.Failed.append((task.Path, error))
                self.ErrorCounts[reduced_error] = self.ErrorCounts.get(reduced_error, 0) + 1
        else:
            self.RemovedCount += 1

    @synchronized
    def taskFailed(self, queue, task, exc_type, exc_value, tb):
        if self.Verbose:
            print("taskFailed:", task.Path, exc_value)
        self.Failed.append((task.Path, str(exc_value)))


def parents(path):
    # produce list of all parents for the path
    while path and path != '/' and '/' in path:
        path = path.rsplit('/', 1)[0]
        yield path

def remove_from_file(file_path, rse, out, lfn_converter, stats, stats_key, dry_run, client, my_stats, verbose, limit):
    paths = [l.strip() for l in open(file_path, "r")]
    failed = Remover(client, paths, verbose=verbose, limit=limit).run()
    for path, error in failed:
        print("Failed:", path, error)
    return my_stats

def update_confirmed(confirmed, update):
    new_confirmed = confirmed & update
    unconfirmed = confirmed - update
    for path in unconfirmed:
        for parent in parents(path):
            try:    new_confirmed.remove(parent)
            except KeyError:    pass
    return new_confirmed

def empty_action(storage_path, rse, out, lfn_converter, stats, stats_key, dry_run, client, my_stats, verbose, limit):

    my_stats["start_time"] = t0 = time.time()
    if stats is not None:
        stats.update_section(stats_key, my_stats)

    runs = list(CCRun.runs_for_rse(storage_path, rse, complete_only=False))
    now = datetime.now()

    for r in runs:
        print(r.Run, ":  in window:", r.Timestamp >= now - timedelta(days=window),
                "  ED info collected:", r.empty_directories_collected(),
                "  count:", r.empty_directory_count(),
                "  list present:", r.empty_dir_list_exists()
        )
    recent_runs = sorted(
            [r for r in runs
                if True
                    #and (print(r.Run, r.Timestamp >= now - timedelta(days=window), r.empty_directories_collected(), r.empty_directory_count()) or True)
                    and (r.Timestamp >= now - timedelta(days=window))
                    and r.empty_directories_collected()
                    and r.empty_directory_count() is not None
                    and r.empty_dir_list_exists()
                    #and (print(r.Run, r.Timestamp >= now - timedelta(days=window), r.empty_directories_collected(), r.empty_directory_count()) or True)
            ],
            key=lambda r: r.Timestamp
    )

    print("Usable runs:")
    for r in recent_runs:
        print("  ", r.Run)

    status = "started"
    aborted_reason = None
    confirmed_empty_count = None
    detected_empty_count = None
    failed_count = 0
    removed_count = 0
    error_counts = {}
    error = None

    if recent_runs:
        my_stats["runs_compared"] = [r.Run for r in recent_runs]

    if not recent_runs or len(recent_runs) < min_runs:
        status = "aborted"
        aborted_reason = "not enough runs to produce confirmed empty directories list: %d, required: %d" % (len(recent_runs), min_runs)
    else:
        first_run = recent_runs[0]
        latest_run = recent_runs[-1]
        num_scanned = latest_run.scanner_num_files()
        detected_empty_count = latest_run.empty_directory_count()
        print("Runs in the confirmation history:", len(recent_runs))
        print("First run:", first_run.Run, file=sys.stderr)
        print("Last run:", latest_run.Run, file=sys.stderr)
        print("Empty directories in last run:", latest_run.empty_directory_count(), file=sys.stderr)

        if latest_run.Timestamp < now - timedelta(days=max_age_last):
            status = "aborted"
            aborted_reason = "latest run is too old: %s, required: < %d days old" % (latest_run.Timestamp, max_age_last)

        elif first_run.Timestamp > now - timedelta(days=min_age_first):
            status = "aborted"
            aborted_reason = "oldest run is not old enough: %s, required: > %d days old" % (first_run.Timestamp, min_age_first)

        else:
            # compute confirmed list and make sure the list would contain only removable directories
            confirmed = set(lfn_converter.lfn_or_path_to_path(path) for path in recent_runs[0].empty_directories())
            confirmed = update_confirmed(confirmed, set(lfn_converter.lfn_or_path_to_path(path) for path in recent_runs[-1].empty_directories()))
            for run in recent_runs[1:-1]:
                print(f"run: {run} - #confirmed: {len(confirmed)}")
                if not confirmed:
                    break
                run_set = set(lfn_converter.lfn_or_path_to_path(path) for path in run.empty_directories())
                confirmed = update_confirmed(confirmed, run_set)

            confirmed_empty_count = len(confirmed)
            print("Confirmed empty directories:", confirmed_empty_count, file=sys.stderr)

            my_stats.update(
                detected_empty_directories = detected_empty_count,
                confirmed_empty_directories = confirmed_empty_count
            )
            if stats is not None:
                stats.update_section(stats_key, my_stats)

            status = "done"
            if confirmed:
                if out is not None:
                    for f in sorted(confirmed):
                        print(f, file=out)
                    if out is not sys.stdout:
                        out.close()
                try:
                    remover = Remover(client, confirmed, dry_run, verbose=verbose, limit=limit)
                    failed = remover.run()
                    failed_count = len(failed)
                    removed_count = remover.RemovedCount
                    error_counts = remover.ErrorCounts
                except Exception as e:
                    error = f"remover error: {e}"
                    status = "failed"

    t1 = time.time()
    my_stats.update(
        elapsed = t1-t0,
        end_time = t1,
        status = status,
        error = error,
        detected_empty_directories = detected_empty_count,
        confirmed_empty_directories = confirmed_empty_count,
        failed_count = failed_count,
        removed_count = removed_count,
        aborted_reason = aborted_reason,
        error_counts = error_counts
    )

    if stats is not None:
        stats.update_section(stats_key, my_stats)

    return my_stats

if not sys.argv[1:] or sys.argv[1] == "help":
    print(Usage)
    sys.exit(2)

opts, args = getopt.getopt(sys.argv[1:], "h?o:M:m:w:n:f:s:S:c:va:dL:")
opts = dict(opts)

if not args or "-h" in opts or "-?" in opts:
    print(Usage)
    sys.exit(2)

out = None
out_path = opts.get("-o")
out_filename = out_path.rsplit('/', 1)[-1] if out_path else None
if out_path:
    if out_path == "-":
        out = sys.stdout
    else:
        out = open(out_path, "w")


storage_path, rse = args

config  = ActionConfiguration(rse, opts["-c"], "dark")
scanner_config = CEConfiguration(opts["-c"])[rse].get("scanner", {})

window = int(opts.get("-w", config.get("confirmation_window", 35)))
min_age_first = int(opts.get("-M", config.get("min_age_first_run", 25)))
max_age_last = int(opts.get("-m", config.get("max_age_last_run", 1)))
fraction = float(opts.get("-f", config.get("max_fraction", 0.01)))
min_runs = int(opts.get("-n", config.get("min_runs", 3)))
account = opts.get("-a")
dry_run = "-d" in opts
verbose = "-v" in opts
limit = opts.get("-L")
if limit:   limit = int(limit)


if dry_run:
    print("====== dry run mode ======")

stats_file = opts.get("-s")
stats = stats_key = None
if stats_file is not None:
    stats = Stats(stats_file)
stats_key = opts.get("-S", "empty_action")

if "-v" in opts:
    print("\nParameters:")
    print("  dry run:                     ", dry_run)
    print("  stats file:                  ", stats_file)
    print("  stats key:                   ", stats_key)
    print("  config:                      ", opts.get("-c"))
    print("  confirmation window:         ", window)
    print("  min age for last run:        ", min_age_first)
    print("  max age for first run:       ", max_age_last)
    print("  min number of runs:          ", min_runs)
    print("  limit:                       ", "no limit" if limit is None else limit)
    print()
    print("Scanner:")
    print("  server:        ", scanner_config["server"])
    print("  serverRoot:    ", scanner_config["server_root"])
    print("  add prefix:    ", scanner_config["add_prefix"])
    print("  remove prefix: ", scanner_config["remove_prefix"])
    print("  timeout:       ", scanner_config["timeout"])
    print()

my_stats = {
    "version": Version,
    "dry_run": dry_run,
    "elapsed": None,
    "start_time": None,
    "end_time": None,
    "status": "started",
    "detected_empty_directories": None,
    "confirmed_empty_directories": None,
    "removed_count": 0,
    "failed_count": 0,
    "confirmed_list": out_filename,
    "aborted_reason": None,
    "error": None,
    "runs_compared": None,
    "limit": limit,
    "configuration": {
        "confirmation_window": window,
        "min_age_first_run": min_age_first,
        "max_age_last_run": max_age_last,
        "min_runs": min_runs
    }
}

if stats is not None:
    stats.update_section(stats_key, my_stats)

server        = scanner_config["server"]
server_root   = scanner_config["server_root"]
add_prefix    = scanner_config["add_prefix"]
remove_prefix = scanner_config["remove_prefix"]

lfn_converter = LFNConverter(server_root, remove_prefix, add_prefix)

timeout = scanner_config["timeout"]
is_redirector = False    # config.ServerIsRedirector
client = XRootDClient(server, is_redirector, server_root, timeout=timeout)
if os.path.isfile(storage_path):
    run_stats = remove_from_file(storage_path, rse, out, lfn_converter, stats, stats_key, dry_run, client, my_stats, verbose, limit)
else:
    run_stats = empty_action(storage_path, rse, out, lfn_converter, stats, stats_key, dry_run, client, my_stats, verbose, limit)
status = run_stats["status"]
error = run_stats.get("error")
aborted_reason = run_stats.get("aborted_reason")

if verbose:
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
