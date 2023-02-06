from pythreader import TaskQueue, Task, DEQueue, PyThread, synchronized, ShellCommand, Primitive
import re, json, os, os.path, traceback
import subprocess, time, random, gzip
from part import PartitionedList
from py3 import to_str
from stats import Stats
from xrootd_client import XRootDClient

Version = "3.0"

GB = 1024*1024*1024

try:
    import tqdm
    Use_tqdm = True
except:
    Use_tqdm = False

from config import ScannerConfiguration

def truncated_path(root, path):
        if path == root:
            return "/"
        relpath = path
        if path.startswith(root+"/"):
            relpath = path[len(root)+1:]
        N = 5
        parts = relpath.split("/")
        while parts and not parts[0]:
            parts = parts[1:]
        if len(parts) <= N:
            #return "%s -> %s" % (path, relpath)
            return relpath
        else:
            n = len(parts)
            #return ("%s -> ..(%d)../" % (path, n-N))+"/".join(parts[-N:])
            return ("..(%d)../" % (n-N))+"/".join(parts[-N:])

def canonic_path(path):
    while path and "//" in path:
        path = path.replace("//", "/")
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    return path
    
def relative_path(root, path):
    # returns part relative to the root. Returned relative path does NOT have leading slash
    # if the argument path does not start with root, returns the path unchanged
    path = canonic_path(path)
    if path.startswith(root + "/"):
        path = path[len(root)+1:]
    return path
    
class PathConverter(object):
    
    def __init__(self, site_prefix, remove_prefix, add_prefix, root):
        self.SitePrefix = site_prefix
        self.RemovePrefix = remove_prefix
        self.AddPrefix = add_prefix
        self.Root = root
    
    def path_to_logpath(self, path):
        # convert physical path after site prefix to LFN space by applying RemovePrefix and AddPrefix if any
        # for CMS, this is a no-op as of now
    
        path = canonic_path(path)
        assert path.startswith('/'), f"Expected input path to start with /: {path}"
        if self.RemovePrefix and path.startswith(self.RemovePrefix):
            path = path[len(self.RemovePrefix):]

        if self.AddPrefix:
            path = self.AddPrefix + path

        return canonic_path(path)

class Prescanner(Primitive):

    class PrescannerTask(Task):

        def __init__(self, server, server_root, is_redirector, root, timeout):
            Task.__init__(self, name=f"RootPrescanner({root})")
            self.Client = None
            self.Server = server
            self.ServerRoot = server_root
            self.IsRedirector = is_redirector
            self.Root = root
            self.Timeout = timeout
            self.Failed = False
            self.Error = None

        def run(self):
            self.Client = XRootDClient(self.Server, self.IsRedirector, self.ServerRoot, 
                    timeout=self.Timeout, name=f"XRootDClient({self.Root})")
            self.Client.prescan(self.Root)
            status, self.Error, _, _ = self.Client.ls(self.Root, False, False)
            self.Failed = status != "OK"
            return not self.Failed

    def __init__(self, server, server_root, is_redirector, roots, timeout, max_scanners):
        Primitive.__init__(self)
        self.Good = []              # [client, ...]
        self.Failed = {}            # {root: error}
        self.Queue = TaskQueue(max_scanners, stagger=0.5, delegate=self,
            tasks = [self.PrescannerTask(server, server_root, is_redirector, root, timeout) for root in roots]
        )

    def run(self):
        self.Queue.waitUntilEmpty()
        return self.Good, self.Failed

    @synchronized
    def taskEnded(self, queue, task, root_ok):
        if root_ok:
            self.Good.append((task.Client, task.Root))
            print(f"Root {task.Root} prescanned successfully", file=sys.stderr)
        else:
            self.Failed[task.Root] = task.Error
            print(f"Root prescan for {task.Root} failed with error:", task.Error, file=sys.stderr)

    @synchronized
    def taskFailed(self, queue, task, exc_type, exc_value, tb):
        self.Failed[task.Root] = "Exception: " + "\n".join(traceback.format_exception_only(exc_type, exc_value))
        print(f"Root prescan for {task.Root} failed with exception:", self.Failed[task.Root], file=sys.stderr)

class Scanner(Task):
    
    MAX_ATTEMPTS_REC = 2
    MAX_ATTEMPTS_FLAT = 3

    def __init__(self, master, client, location, recursive, include_sizes = True, report_empty_top = True):
        Task.__init__(self)
        self.Client = client
        self.Master = master
        self.Location = canonic_path(location)
        self.ForcedFlat = not recursive
        self.WasRecursive = recursive
        self.Subprocess = None
        self.Killed = False
        self.Started = None
        self.Elapsed = None
        self.RecAttempts = self.MAX_ATTEMPTS_REC if recursive else 0
        self.FlatAttempts = self.MAX_ATTEMPTS_FLAT
        self.IncludeSizes = include_sizes
        self.ReportEmptyTop = report_empty_top
        #print("Scanner create for location:", self.Location)

    def __str__(self):
        return "Scanner(%s)" % (self.Location,)

    def message(self, status, stats):
        if self.Master is not None:
            self.Master.message("%s %s %s" % (status, stats, self.Location))

    @synchronized
    def killme(self):
        if self.Subprocess is not None:
                self.Killed = True
                self.Subprocess.terminate()
                
    def parent(self, path):
        words = path.rsplit('/', 1)
        if len(words) == 1:
            return ""               # relative path ??
        return words[0] or "/"
                
    def run(self):
        #print("Scanner.run():", self.Master)
        self.Started = t0 = time.time()
        location = self.Location
        if self.RecAttempts > 0:
            recursive = True
            self.RecAttempts -= 1
        else:
            recursive = False
            self.FlatAttempts -= 1
        self.WasRecursive = recursive
        #self.message("start", stats)

        # Location is relative to the server root, it does start with '/'. E.g. /store/mc/run2
        status, reason, dirs, files = self.Client.ls(self.Location, recursive, self.IncludeSizes)
        # paths are relative to the Server Root, they do start with '/', e.g. /store/mc/run2/data.file
        files = list(files)
        dirs = list(dirs)
        self.Elapsed = time.time() - self.Started
        if status != "OK":
            stats += " " + reason
            self.message(status, stats)
            if self.Master is not None:
                self.Master.scanner_failed(self, f"{status}: {reason}")
            return

        #stats = "%1s %7.3fs" % ("r" if recursive else " ", self.Elapsed)
        stats = "r" if recursive else " "
    
        #
        # create the set of directories, which contain no files, recursively
        #
        empty_dirs = set()
        if recursive:
            empty_dirs = set(p for p, _ in dirs)
            for path, _ in files:
                dirpath = self.parent(path)
                while dirpath and dirpath != '/' and dirpath in empty_dirs:
                    empty_dirs.remove(dirpath)
                    dirpath = self.parent(dirpath)

        if self.ReportEmptyTop and (recursive or not dirs) and not files:
            empty_dirs.add(self.Location)

        counts = " files: %-8d dirs: %-8d empty: %-8d" % (len(files), len(dirs), len(empty_dirs))
        if self.IncludeSizes:
            total_size = sum(size for _, size in files) + sum(size for _, size in dirs)
            counts += " size: %10.3fGB" % (total_size/GB,)
        self.message("done", stats+counts)
        if self.Master is not None:
            self.Master.scanner_succeeded(location, self.WasRecursive, files, dirs, empty_dirs)

class ScannerMaster(PyThread):
    
    MAX_RECURSION_FAILED_COUNT = 5
    REPORT_INTERVAL = 10.0
    RESULTS_BUFFER_SISZE = 100
    
    def __init__(self, client, path_converter, root, recursive_threshold, max_scanners, timeout, quiet, display_progress, max_files = None,
                include_sizes=True, ignore_list=[]):
        PyThread.__init__(self)
        self.RecursiveThreshold = recursive_threshold
        self.PathConverter = path_converter
        self.Client = client
        self.Root = root
        self.MaxScanners = max_scanners
        self.Results = DEQueue(self.RESULTS_BUFFER_SISZE)
        self.ScannerQueue = TaskQueue(max_scanners, stagger=0.2)
        self.Done = False
        self.Error = None
        self.Failed = False
        self.GaveUp = {}
        self.LastReport = time.time()
        self.NEmptyDirs = 0
        self.NScanned = 0
        self.NToScan = 1 
        self.Quiet = quiet
        self.DisplayProgress = display_progress and Use_tqdm and not quiet
        if self.DisplayProgress:
            self.TQ = tqdm.tqdm(total=self.NToScan, unit="dir")
            self.LastV = 0
        self.NFiles = self.NDirectories = 0
        self.MaxFiles = max_files       # will stop after number of files found exceeds this number. Used for debugging
        self.IgnoreList = ignore_list
        self.IgnoredFiles = self.IgnoredDirs = 0
        self.IncludeSizes = include_sizes
        self.TotalSize = 0.0 if include_sizes else None                  # Megabytes

    def run(self):
        #
        # scan Root non-recursovely first, if failed, return immediarely
        #
        #server, location, recursive, timeout
        scanner_task = Scanner(self, self.Client, self.Root, self.RecursiveThreshold == 0, include_sizes=self.IncludeSizes, report_empty_top=False)
        self.ScannerQueue.addTask(scanner_task)
        
        self.ScannerQueue.waitUntilEmpty()
        self.Results.close()
        self.ScannerQueue.Delegate = None       # detach for garbage collection
        self.ScannerQueue = None
        
    def dir_ignored(self, logpath):
        # path is expected to be canonic here
        return any((logpath == subdir or logpath.startswith(subdir+"/")) for subdir in self.IgnoreList)

    def file_ignored(self, logpath):
        # path is expected to be canonic here
        return any(logpath.startswith(subdir+"/") for subdir in self.IgnoreList) or logpath in self.IgnoreList

    def addDirectoryToScan(self, logpath, allow_recursive):
        if not self.Failed:
            relpath = logpath[len(self.Root):]
            reldepth = len([w for w in relpath.split('/') if w])

            allow_recursive = allow_recursive and (self.RecursiveThreshold is not None 
                and reldepth >= self.RecursiveThreshold 
            )

            if self.MaxFiles is None or self.NFiles < self.MaxFiles:
                self.ScannerQueue.addTask(
                    Scanner(self, self.Client, logpath, allow_recursive, include_sizes=self.IncludeSizes)
                )
                self.NToScan += 1

    def addEmptyDirectories(self, paths):
        if not self.Failed:
            for path in paths:
                if path != self.Root:
                    # do not report root even if it is empty
                    self.Results.append(('e', path))
                    self.NEmptyDirs += 1
                else:
                    print("Empty root", path,"removed from empty list")

    @synchronized
    def report(self):
        if time.time() > self.LastReport + self.REPORT_INTERVAL:
            waiting, active = self.ScannerQueue.tasks()
            #sys.stderr.write("--- Locations to scan: %d\n" % (len(active)+len(waiting),))
            self.LastReport = time.time()

    @synchronized
    def scanner_failed(self, scanner, error):
        path = scanner.Location                
        retry = (scanner.RecAttempts > 0) or (scanner.FlatAttempts > 0)
        if retry:
            print("resubmitted:", scanner.Location, scanner.RecAttempts, scanner.FlatAttempts)
            self.ScannerQueue.addTask(scanner)
        else:
            print("Gave up:", scanner.Location)
            self.GaveUp[scanner.Location] = error
            self.NScanned += 1  
            #sys.stderr.write("Gave up on: %s\n" % (path,))
            self.show_progress()            #"Error scanning %s: %s -- retrying" % (scanner.Location, error))
        
    @synchronized
    def scanner_succeeded(self, location, was_recursive, files, dirs, empty_dirs):
        self.NScanned += 1
        for path, size in files:
            logpath = self.PathConverter.path_to_logpath(path)
            self.NFiles += 1
            if not self.file_ignored(logpath):
                self.Results.append(('f', logpath))
                self.TotalSize += size
            else:
                self.IgnoredFiles += 1

        scan = not was_recursive
        allow_recursive = scan and len(dirs) > 1
        for path, size in dirs:
            logpath = self.PathConverter.path_to_logpath(path)
            self.NDirectories += 1
            if self.dir_ignored(logpath):
                self.IgnoredDirs += 1
                if scan:    print(logpath, " - directory ignored")
            else:
                self.Results.append(('d', logpath))
                if scan:
                    self.addDirectoryToScan(logpath, allow_recursive)

        if empty_dirs:
            self.addEmptyDirectories(empty_dirs)            # do not convert to LFN

        self.show_progress()

    def paths(self):
        # log paths, e.g. /store/mc/...
        for t, path in self.Results:
            yield t, path

    @synchronized
    def show_progress(self, message=None):
        if self.DisplayProgress:
            self.TQ.total = self.NToScan
            delta = max(0, self.NScanned - self.LastV)
            self.TQ.update(delta)
            self.LastV = self.NScanned
            enf = 0
            if self.NScanned > 0:
                enf = int(self.NFiles * self.NToScan/self.NScanned)
            self.TQ.set_postfix(f=self.NFiles, ed=self.NEmptyDirs, d=self.NDirectories, enf=enf)
            if message:
                self.TQ.write(message)   
                
    @synchronized
    def message(self, message):
        if not self.Quiet:
                if self.DisplayProgress:
                    self.TQ.write(message)
                else:
                    print(message)
                    sys.stdout.flush()

    def close_progress(self):
        if self.DisplayProgress:
            self.TQ.close()
                
Usage = """
python xrootd_scanner.py [options] <rse>
    Options:
    -c <config.yaml>|-c rucio   - required - read config either from a YAML file or from Rucio
    -o <output file prefix>     - output will be sent to <output>.00000, <output>.00001, ...
    -e <path>                   - output file for the list of empty directories
    -t <timeout>                - xrdfs ls operation timeout (default 30 seconds)
    -m <max workers>            - default 5
    -R <recursion depth>        - start using -R at or below this depth (dfault 3)
    -n <nparts>
    -k                          - do not treat individual directories scan errors as overall scan failure
    -q                          - quiet - only print summary
    -x                          - do not use metadata (ls -l), do not include file sizes
    -M <max_files>              - stop scanning the root after so many files were found
    -s <stats_file>             - write final statistics to JSON file
"""

def path_to_lfn(path, path_prefix, remove_prefix, add_prefix, path_filter, rewrite_path, rewrite_out):
    # convert absoulte physical path, which starts with path_prefix to LFN
    # for CMS, path may look like /eos/cms/tier0/store/root/path/file
    # after removing the <path_prefix>, then <remove_prefix> and adding <add_prefix> it will look like /store/root/path/file
    
    assert path.startswith(path_prefix)

    lfn = "/" + path[len(path_prefix):]

    if remove_prefix and lfn.startswith(remove_prefix):
        lfn = lfn[len(remove_prefix):]

    if add_prefix:
        lfn = add_prefix + lfn

    if path_filter:
        if not path_filter.search(lfn):
            return None

    if rewrite_path is not None:
        if not rewrite_path.search(lfn):
            sys.stderr.write(f"Path rewrite pattern for root {root} did not find a match in path {lfn}\n")
            sys.exit(1)
        lfn = rewrite_path.sub(rewrite_out, lfn)   
    return lfn

def scan_root(rse, config, client, root, my_stats, stats, stats_key,
            recursive_threshold, max_scanners, file_list, dir_list, empty_dirs_file,
            ignore_failed_directories, include_sizes):

    failed = root_failed = False
    
    timeout = override_timeout or config.ScannerTimeout
    server = config.Server
    server_root = config.ServerRoot
    max_scanners = override_max_scanners or config.NWorkers
    ignore_subdirs = config.ignore_subdirs(root)
    is_redirector = config.ServerIsRedirector
    ignore_list = config.IgnoreList

    t0 = time.time()
    root_stats = {
       "root": root,
       "start_time":t0,
       "timeout":timeout,
       "recursive_threshold":recursive_threshold,
       "max_scanners":max_scanners,
       "ignore_subdirectories": ignore_subdirs,
       "servers": client.Servers
    }

    my_stats["scanning"] = root_stats
    if stats is not None:
        stats.update_section(stats_key, my_stats)

    remove_prefix = config.RemovePrefix
    add_prefix = config.AddPrefix
    path_converter = PathConverter(server_root, remove_prefix, add_prefix, root)

    master = ScannerMaster(client, path_converter, root, recursive_threshold, max_scanners, timeout, quiet, display_progress,
            max_files = max_files, include_sizes=include_sizes,
            ignore_list = ignore_list)

    path_filter = None          # -- obsolete -- config.scanner_filter(rse)
    #if path_filter is not None:
    #    path_filter = re.compile(path_filter)
    rewrite_path, rewrite_out = None, None      # -- obsolete -- config.scanner_rewrite(rse)
    if rewrite_path is not None:
        assert rewrite_out is not None
        rewrite_path = re.compile(rewrite_path)

    print("Starting scan of %s:%s with:" % (server, root))
    print("  Include sizes       = %s" % include_sizes)
    print("  Recursive threshold = %d" % (recursive_threshold,))
    print("  Max scanner threads = %d" % max_scanners)
    print("  Timeout             = %s" % timeout)
    if ignore_list:
        print("  Ignore list:")
        for p in ignore_list:
            print("    ", p)

    master.start()

    path_prefix = server_root
    if not path_prefix.endswith("/"):
        path_prefix += "/"

    for t, logpath in master.paths():
        # here, path is absolute path, which includes site root
        if t == 'f':
            file_list.add(logpath)             
        elif t == 'd' and dir_list is not None:
            dir_list.add(logpath)
        elif t == 'e' and empty_dirs_file is not None:
            empty_dirs_file.write(logpath)
            empty_dirs_file.write("\n")

    if display_progress:
        master.close_progress()

    if master.Failed:
        sys.stderr.write("Scanner failed to scan %s: %s\n" % (root, master.Error))

    if master.GaveUp:
        sys.stderr.write("Scanner failed to scan the following %d locations:\n" % (len(master.GaveUp),))
        for path, error in sorted(list(master.GaveUp.items())):
            sys.stderr.write(f"{path}: {error}\n")

    print("Files:                %d" % (master.NFiles,))
    print("Files ignored:        %d" % (master.IgnoredFiles,))
    print("Directories found:    %d" % (master.NToScan,))
    print("Directories ignored:  %d" % (master.IgnoredDirs,))
    print("Directories scanned:  %d" % (master.NScanned,))
    print("Directories:          %d" % (master.NDirectories,))
    print("  empty directories:  %d" % (master.NEmptyDirs,))
    print("Failed directories:   %d" % (len(master.GaveUp),))
    if include_sizes:
        print("Total size:           %.3f GB" % (master.TotalSize/GB))
    t1 = time.time()
    elapsed = int(t1 - t0)
    s = elapsed % 60
    m = elapsed // 60
    print("Elapsed time:         %dm %02ds\n" % (m, s))
    
    if (not ignore_failed_directories) and master.GaveUp:
        failed = True

    total_size = None if failed or master.TotalSize is None else master.TotalSize/GB

    root_stats.update({
        "root_failed": False,
        "error": master.Error,
        "failed_subdirectories": master.GaveUp,
        "files": master.NFiles,
        "directories": master.NDirectories,
        "empty_directories": master.NEmptyDirs,
        "directories_ignored": master.IgnoredDirs,
        "files_ignored": master.IgnoredFiles,
        "end_time":t1,
        "elapsed_time": t1-t0,
        "total_size_gb": total_size,
        "ignored_subdirectories": ignore_subdirs,
        "servers": client.Servers
    })

    del my_stats["scanning"]
    my_stats["roots"].append(root_stats)
    if stats is not None:
        stats[stats_key] = my_stats
        if failed:
            stats["error"] = root_stats.get("error")
    return failed
    
if __name__ == "__main__":
    import getopt, sys, time

    t0 = time.time()    
    opts, args = getopt.getopt(sys.argv[1:], "t:m:o:R:n:c:vqM:s:S:zd:kxe:")
    opts = dict(opts)
    
    if len(args) != 1 or not "-c" in opts:
        print(Usage)
        sys.exit(2)

    rse = args[0]
    config = ScannerConfiguration(rse, opts["-c"])

    quiet = "-q" in opts
    display_progress = not quiet and "-v" in opts
    override_recursive_threshold = int(opts.get("-R", 0))
    override_timeout = int(opts.get("-t", 0))
    override_max_scanners = int(opts.get("-m", 0))
    max_files = opts.get("-M")
    if max_files is not None: max_files = int(max_files)
    stats_file = opts.get("-s")
    stats_key = opts.get("-S", "scanner")
    ignore_directory_scan_errors = "-k" in opts
    
    stats = None if not stats_file else Stats(stats_file)
    
    zout = "-z" in opts
    
    if "-n" in opts:
        nparts = int(opts["-n"])
    else:
        nparts = config.NPartitions

    if nparts > 1:
        if not "-o" in opts:
            print ("Output prefix is required for partitioned output")
            print (Usage)
            sys.exit(2)

    output = opts.get("-o","out.list")

    out_list = PartitionedList.create(nparts, output, zout)

    dir_output = opts.get("-d")
    dir_list = PartitionedList.create(nparts, dir_output, zout) if dir_output else None
    
    empty_dirs_file = None
    empty_dir_output = opts.get("-e")
    if empty_dir_output:
        if zout:
            if not empty_dir_output.endswith(".gz"):    empty_dir_output += ".gz"
            empty_dirs_file = gzip.open(empty_dir_output, "wt")
        else:
            empty_dirs_file = open(empty_dir_output, "w")

    server = config.Server
    server_root = config.ServerRoot
    include_sizes = config.IncludeSizes and not "-x" in opts
    if not server_root:
        print(f"Server root is not defined for {rse}. Should be defined as 'server_root'")
        sys.exit(2)

    my_stats = {
        "rse":rse,
        "scanner":{
            "type":"xrootd",
            "version":Version
        },
        "server_root":server_root,
        "server":server,
        "roots":[], 
        "start_time":time.time(),
        "end_time": None,
        "status":   "started",
        "files_output_prefix":          output,
        "dirs_output_prefix":           dir_output,
        "empty_dirs_output_file":       empty_dir_output
    }
    
    if stats is not None:
        stats[stats_key] = my_stats
    
    max_scanners = override_max_scanners or config.NWorkers
    recursive_threshold = override_recursive_threshold or config.RecursionThreshold
    root_paths = [canonic_path(root if root.startswith("/") else server_root + "/" + root) for root in config.RootList]
    
    t0 = time.time()
    good_roots, failed_roots = Prescanner(server, server_root, config.ServerIsRedirector, config.RootList, config.ScannerTimeout, max_scanners).run()
    t1 = time.time()
    
    my_stats["roots"] = [
        {
            "root": root,
            "start_time":t0,
            "timeout":config.ScannerTimeout,
            "root_failed": True,
            "error": error,
            "end_time":t1,
            "files": 0,
            "directories": 0,
            "elapsed_time": t1-t0
        }
        for root, error in failed_roots.items()
    ]

    failed = False
    all_roots_failed = not good_roots
    for client, root in good_roots:
        try:
            print(f"Scanning root {root} ...", file=sys.stderr)
            failed = scan_root(rse, config, client, root, my_stats, stats, stats_key, recursive_threshold, 
                    max_scanners, out_list, dir_list, empty_dirs_file,
                    ignore_directory_scan_errors, include_sizes)
        except:
            exc = traceback.format_exc()
            print(exc)
            lines = exc.split("\n")
            scanning = my_stats.setdefault("scanning", {"root":client.Root})
            scanning["exception"] = lines
            scanning["exception_time"] = time.time()
            failed = True

        if failed:
            break

    out_list.close()
    if dir_list is not None:
        dir_list.close()
    if empty_dirs_file is not None:
        empty_dirs_file.close()

    total_files = sum(root_stats["files"] for root_stats in my_stats["roots"])
    if failed or all_roots_failed or total_files == 0:
        my_stats["status"] = "failed"
    else:
        my_stats["status"] = "done"
        
    my_stats["end_time"] = t1 = time.time()
    my_stats["elapsed"] = t1 - my_stats["start_time"]
    if stats is not None:
        stats[stats_key] = my_stats

    if failed or all_roots_failed:
        sys.exit(1)
    else:
        sys.exit(0)
