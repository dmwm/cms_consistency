from pythreader import TaskQueue, Task, DEQueue, PyThread, synchronized, ShellCommand
import re, json, os, os.path, traceback
import subprocess, time
from part import PartitionedList
from py3 import to_str
from stats import Stats

Version = "2.0"

GB = 1024*1024*1024

try:
    import tqdm
    Use_tqdm = True
except:
    Use_tqdm = False

from config import Config

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


class RMDir(Task):
    
    # a task to delete presumably empty subdirectory discovered by the scanner
    
    def __init__(self, server, location):
        self.Server = server
        self.Location = location
    
    TIMEOUT = 5
    
    def run(self):

        rmcommand = "xrdfs %s rmdir %s" % (self.Server, self.Location)
        try:    
            print(f"would delete directory {self.Location} with {rmcommand}")
            #ShellCommand.execute(rmcommand, timeout=self.TIMEOUT)
        except: 
            # ignore
            pass
        
class Scanner(Task):
    
    MAX_ATTEMPTS_REC = 1
    MAX_ATTEMPTS_FLAT = 3

    def __init__(self, master, server, location, recursive, timeout, include_sizes = True):
        Task.__init__(self)
        self.Server = server
        self.Master = master
        self.Location = canonic_path(location)
        self.Timeout = timeout
        self.WasRecursive = self.Recursive = recursive
        self.Subprocess = None
        self.Killed = False
        self.Started = None
        self.Elapsed = None
        self.RecAttempts = self.MAX_ATTEMPTS_REC if recursive else 0
        self.FlatAttempts = self.MAX_ATTEMPTS_FLAT
        self.IncludeSizes = include_sizes

    def __str__(self):
        return "Scanner(%s)" % (self.Location,)

    def message(self, status, stats):
        if self.Master is not None:
            self.Master.message("%-100s\t%s %s" % (truncated_path(self.Master.Root, self.Location), status, stats))
        

    @synchronized
    def killme(self):
        if self.Subprocess is not None:
                self.Killed = True
                self.Subprocess.terminate()
                
    #dr-x 2021-06-23 23:21:46           0 /store/unmerged/HINPbPbSpring21MiniAOD

    Line_Patterns = [
        r"""
                (?P<mask>[drwx-]{4})\s+
                \d{4}-\d{2}-\d{2}\s+
                \d{2}:\d{2}:\d{2}\s+
                (?P<size>\d+)\s+
                (?P<path>[^ ]+)
        """,
        r"""
                (?P<mask>[drwx-]{4})\s+
                \d{4}-\d{2}-\d{2}\s+
                \d{2}:\d{2}:\d{2}\s+
                (?P<size>\d+)\s+
                (?P<path>[^ ]+)
        """
    ]
    
    Line_Patterns = [re.compile(p, re.VERBOSE) for p in Line_Patterns]

    def parse_scan_line(self, line, with_meta):
        """
        returns (is_file, size, path)
        
        dr-x 2021-07-13 04:00:26        4096 /store/unmerged//Run2016B//DoubleEG//MINIAOD//21Feb2020_ver2_UL2016_HIPM-v2//280004
        -r-- 2021-07-02 09:35:03   719124843 /store/unmerged//Run2016B//DoubleEG//MINIAOD//21Feb2020_ver2_UL2016_HIPM-v2//280004//1C576248-6EEF-B74F-A336-D1D5B8E41722.root
        
        drwxrwxr-x root root 0 2021-06-23 23:21:46 /store/unmerged/HINPbPbSpring21MiniAOD
        """
        if with_meta:
            line = line.strip()
            is_file = size = path = None
            for p in self.Line_Patterns:
                m = p.match(line)
                if m:
                    is_file = m.group("mask")[0] != 'd'
                    size = int(m.group("size"))
                    path = canonic_path(m.group("path"))
                    break
            else:
                return None
        else:
            size = None
            path = line.strip()
            last_item = path.rsplit("/",1)[-1]
            is_file = (not last_item in (".", "..")) and "." in last_item
        #print("parse:", line,"->",is_file, size, canonic_path(path))
        return is_file, size, canonic_path(path)

    def scan(self, recursive, with_meta):
        #print(f"scan({self.Location}, rec={recursive}, with_meta={with_meta}...")
        server = self.Server
        location = self.Location
        timeout = self.Timeout
        lscommand = "xrdfs %s ls %s %s %s" % (server, "-l" if with_meta else "", "-R" if recursive else "", location)        
        files = []
        dirs = []
        status = "OK"
        reason = ""


        try:    
            #print(f"lscommand: {lscommand}")
            retcode, out, err = ShellCommand.execute(lscommand, timeout=timeout)
            #print(f"retcode: {retcode}")
        except RuntimeError:
            status = "timeout"
        else:
            if retcode:
                if "not a directory" in err.lower():
                    return "OK", "", [], [location]

                status = "ls failed"
                reason = "status code: %s, stderr: [%s]" % (retcode, err)

                command = "xrdfs %s stat %s" % (server, location)
                subp = subprocess.Popen(command, shell=True, 
                                stderr=subprocess.PIPE,
                                stdout=subprocess.PIPE)

                subp_out, subp_err = subp.communicate()
                subp_out = to_str(subp_out)
                subp_err = to_str(subp_err)
                retcode = subp.returncode
            
                if retcode:
                    status = "stat failed"
                    reason = "status: %d" % (retcode,)
                    if subp_err: reason += " " + subp_err.strip()
                else:
                    for line in subp_out.split("\n"):
                        line = line.strip()
                        if line.startswith("Flags:"):
                            if not ("IsDir" in line):
                                files = [(location, None)]
                                status = "OK"
                                reason = ""
                            break
            else:
                lines = [x.strip() for x in out.split("\n")]
                for l in lines:
                    if not l: continue
                    tup = self.parse_scan_line(l, with_meta)
                    if not tup or not tup[-1].startswith(location):
                        status = "failed"
                        reason = "Invalid line in output: %s" % (l,)
                        break
                    is_file, size, path = tup
                    if path.endswith("/."):
                        continue
                    path if path.startswith(location) else location + "/" + path
                    if is_file:
                        files.append((path, size))
                    else:
                        dirs.append((path, size))

        if False:
            print("return from scan():")
            print("  dirs:")
            for d in dirs[:5]:
                print("     ", d)
            print("  files:")
            for f in files[:5]:
                print("     ", f)
        
        return status, reason, dirs, files

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
        stats = "r" if recursive else " "
        #self.message("start", stats)

        status, reason, dirs, files = self.scan(recursive, self.IncludeSizes)
        self.Elapsed = time.time() - self.Started
        #stats = "%1s %7.3fs" % ("r" if recursive else " ", self.Elapsed)
        stats = "r" if recursive else " "
        
        if status != "OK":
            stats += " " + reason
            self.message(status, stats)
            if self.Master is not None:
                self.Master.scanner_failed(self, f"{status}: {reason}")

        else:
            counts = " %8d %-8d" % (len(files), len(dirs))
            if self.IncludeSizes:
                total_size = sum(size for _, size in files) + sum(size for _, size in dirs)
                counts += " %.3f" % (total_size/GB,)
            self.message("done", stats+counts)
            if self.Master is not None:
                self.Master.scanner_succeeded(location, recursive, files, dirs)
        
                
    @staticmethod
    def location_exists(server, location, timeout):
        s = Scanner(None, server, location, False, timeout)
        status, reason, dirs, files = s.scan(False, False)
        return status == "OK", reason or status
                
class ScannerMaster(PyThread):
    
    MAX_RECURSION_FAILED_COUNT = 5
    REPORT_INTERVAL = 10.0
    
    def __init__(self, server, root, recursive_threshold, max_scanners, timeout, quiet, display_progress, ignore_patterns=([],[]), max_files = None,
                include_sizes=True):
        PyThread.__init__(self)
        self.RecursiveThreshold = recursive_threshold
        self.Server = server
        self.Root = canonic_path(root)
        self.MaxScanners = max_scanners
        self.Results = DEQueue()
        self.ScannerQueue = TaskQueue(max_scanners)
        self.Timeout = timeout
        self.Done = False
        self.Error = None
        self.Failed = False
        self.RootFailed = False
        self.Directories = set()
        self.RecursiveFailed = {}       # parent path -> count
        self.Errors = {}                # location -> count
        self.GaveUp = {}
        self.LastReport = time.time()
        self.EmptyDirs = set()
        self.NScanned = 0
        self.NToScan = 1 
        self.Quiet = quiet
        self.DisplayProgress = display_progress and Use_tqdm and not quiet
        if self.DisplayProgress:
            self.TQ = tqdm.tqdm(total=self.NToScan, unit="dir")
            self.LastV = 0
        self.NFiles = self.NDirectories = 0
        self.MaxFiles = max_files       # will stop after number of files found exceeds this number. Used for debugging
        self.IgnoreDirPatterns, self.IgnoreFilePatterns = ignore_patterns
        self.IncludeSizes = include_sizes
        self.TotalSize = 0.0 if include_sizes else None                  # Megabytes

    def run(self):
        #
        # scan Root non-recursovely first, if failed, return immediarely
        #
        #server, location, recursive, timeout
        scanner_task = Scanner(self, self.Server, self.Root, self.RecursiveThreshold == 0, self.Timeout, include_sizes=self.IncludeSizes)
        self.ScannerQueue.addTask(scanner_task)
        
        self.ScannerQueue.waitUntilEmpty()
        self.Results.close()
        self.ScannerQueue.Delegate = None       # detach for garbage collection
        self.ScannerQueue = None
        
    def dir_ignored(self, path):
        # path is expected to be canonic here
        return any(p.match(path) is not None for p in self.IgnoreDirPatterns)

    def file_ignored(self, path):
        # path is expected to be canonic here
        return any(p.match(path) is not None for p in self.IgnoreFilePatterns)

    @synchronized
    def addFiles(self, files):
        if not self.Failed:
            self.Results.append(('f', files))
            self.NFiles += len(files)

    def parent(self, path):
        parts = path.rsplit("/", 1)
        if len(parts) < 2:
            return "/"
        else:
            return parts[0]
            
    def addDirectory(self, path, scan):
        if not self.Failed:
            if scan:
                assert path.startswith(self.Root)
                relpath = path[len(self.Root):]
                while relpath and relpath[0] == '/':
                    relpath = relpath[1:]
                while relpath and relpath[-1] == '/':
                    relpath = relpath[:-1]
                reldepth = 0 if not relpath else len(relpath.split('/'))
                
                parent = self.parent(path)
                
                recursive = (self.RecursiveThreshold is not None 
                    and reldepth >= self.RecursiveThreshold 
                )
                #if use_recursive:
                #    print("Use recursive for %s" % (path,))

                if self.MaxFiles is None or self.NFiles < self.MaxFiles:
                    self.ScannerQueue.addTask(
                        Scanner(self, self.Server, path, recursive, self.Timeout, include_sizes=self.IncludeSizes)
                    )
                    self.NToScan += 1
        
    def addDirectories(self, dirs, scan=True):
        if not self.Failed:
            self.Results.append(('d', dirs))
            self.NDirectories += len(dirs)
            for d in dirs:
                d = canonic_path(d)
                if not self.dir_ignored(d):
                    self.addDirectory(d, scan)
            self.show_progress()
            self.report()
            
    @synchronized
    def report(self):
        if time.time() > self.LastReport + self.REPORT_INTERVAL:
            waiting, active = self.ScannerQueue.tasks()
            #sys.stderr.write("--- Locations to scan: %d\n" % (len(active)+len(waiting),))
            self.LastReport = time.time()

    @synchronized
    def scanner_failed(self, scanner, error):
        path = scanner.Location
        if scanner.WasRecursive:
            with self:
                parent = self.parent(path)
                nfailed = self.RecursiveFailed.get(parent, 0)
                self.RecursiveFailed[parent] = nfailed + 1
                
        if not scanner.WasRecursive:
            with self:
                nerrors = self.Errors.setdefault(path, 0)
                nerrors = self.Errors[path] = nerrors + 1
                
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
    def scanner_succeeded(self, location, recursive, files, dirs):
        with self:
            if len(files) == 0 and (recursive or len(dirs) == 0):
                self.EmptyDirs.add(location)
            else:
                if location in self.EmptyDirs:
                    self.EmptyDirs.remove(location)
            if recursive:
                parent = self.parent(location)
                nfailed = self.RecursiveFailed.get(parent, 0)
                self.RecursiveFailed[parent] = nfailed - 1      
        self.NScanned += 1
        if files:
            paths, sizes = zip(*files)
            self.addFiles(paths)
            #for path, size in files:
            #    print(f"path: {path}, size:{size}")
            print("total size (GB):", sum(sizes), location)
            if self.IncludeSizes:
                self.TotalSize += sum(sizes)
        if dirs:
            paths, sizes = zip(*dirs)
            self.addDirectories(paths, not recursive)
            if self.IncludeSizes:
                self.TotalSize += sum(sizes)
        self.show_progress()

    def files(self):
        yield from self.paths('f')
                        
    def paths(self, type=None):
        for t, lst in self.Results:
            if lst and (type is None or type == t):
                for path in lst:
                    path = canonic_path(path)
                    if not self.file_ignored(path):
                        if type is None:
                            yield t, path
                        else:
                            yield path
                        
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
            self.TQ.set_postfix(f=self.NFiles, ed=len(self.EmptyDirs), d=self.NDirectories, enf=enf)
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
                
    def purgeEmptyDirs(self):
        if self.EmptyDirs:
            queue = TaskQueue(self.MaxScanners)
            for path in self.EmptyDirs:
                queue.addTask(RMDir(self.Server, path))
            queue.waitUntilEmpty()
            
Usage = """
python xrootd_scanner.py [options] <rse>
    Options:
    -c <config.yaml>            - config file, required
    -o <output file prefix>     - output will be sent to <output>.00000, <output>.00001, ...
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

def rewrite(path, path_prefix, remove_prefix, add_prefix, path_filter, rewrite_path, rewrite_out):
    
    assert path.startswith(path_prefix)

    path = "/" + path[len(path_prefix):]

    if remove_prefix and path.startswith(remove_prefix):
        path = path[len(remove_prefix):]

    if add_prefix:
        path = add_prefix + path

    if path_filter:
        if not path_filter.search(path):
            return None

    if rewrite_path is not None:
        if not rewrite_path.search(path):
            sys.stderr.write(f"Path rewrite pattern for root {root} did not find a match in path {path}\n")
            sys.exit(1)
        path = rewrite_path.sub(rewrite_out, path)   
    return path
    

def scan_root(rse, root, config, my_stats, stats, stats_key, override_recursive_threshold, override_max_scanners, file_list, dir_list,
    purge_empty_dirs, ignore_failed_directories, include_sizes):
    
    failed = root_failed = False
    
    timeout = override_timeout or config.scanner_timeout(rse)
    top_path = root if root.startswith("/") else server_root + "/" + root
    recursive_threshold = override_recursive_threshold or config.scanner_recursion_threshold(rse, root)
    max_scanners = override_max_scanners or config.scanner_workers(rse)

    t0 = time.time()
    root_stats = {
       "root": root,
       "start_time":t0,
       "timeout":timeout,
       "recursive_threshold":recursive_threshold,
       "max_scanners":max_scanners
    }

    my_stats["scanning"] = root_stats
    if stats is not None:
        stats[stats_key] = my_stats

    exists, reason = Scanner.location_exists(server, top_path, timeout)
    t1 = time.time()
    if not exists:
        print("Root %s does not exist: %s" % (top_path, reason))
        root_stats.update({
            "root_failed": True,
            "error": reason,
            "failed_subdirectories": {},
            "files": 0,
            "directories": 0,
            "empty_directories":0,
            "end_time":t1,
            "elapsed_time": t1-t0
        })
        root_failed = True
    else:
        remove_prefix = config.scanner_remove_prefix(rse)
        add_prefix = config.scanner_add_prefix(rse)
        path_filter = config.scanner_filter(rse)
        if path_filter is not None:
            path_filter = re.compile(path_filter)
        rewrite_path, rewrite_out = config.scanner_rewrite(rse)
        if rewrite_path is not None:
            assert rewrite_out is not None
            rewrite_path = re.compile(rewrite_path)

        print("Starting scan of %s:%s with:" % (server, top_path))
        print("  Include sizes       = %s" % include_sizes)
        print("  Recursive threshold = %d" % (recursive_threshold,))
        print("  Max scanner threads = %d" % max_scanners)
        print("  Timeout             = %s" % timeout)
        
        ignore_list = config.ignore_list(rse)
        if ignore_list:
            print("  Ignore list:")
            for p in ignore_list:
                print("    ", p)
        
        master = ScannerMaster(server, top_path, recursive_threshold, max_scanners, timeout, quiet, display_progress,
            max_files = max_files, ignore_patterns = config.ignore_patterns(rse), include_sizes=include_sizes)
        master.start()

        path_prefix = server_root
        if not path_prefix.endswith("/"):
            path_prefix += "/"
            
        for t, path in master.paths():
            if t == 'f':
                path = rewrite(path, path_prefix, remove_prefix, add_prefix, path_filter, rewrite_path, rewrite_out)
                if path:    
                    file_list.add(path)             
            elif t == 'd' and dir_list is not None:
                path = rewrite(path, path_prefix, remove_prefix, add_prefix, path_filter, rewrite_path, rewrite_out)
                if path:
                    dir_list.add(path) 
                    
        if purge_empty_dirs:
            master.purgeEmptyDirs()

        if display_progress:
            master.close_progress()

        if master.Failed:
            sys.stderr.write("Scanner failed to scan %s: %s\n" % (root, master.Error))
    
        if master.GaveUp:
            sys.stderr.write("Scanner failed to scan the following %d locations:\n" % (len(master.GaveUp),))
            for path, error in sorted(list(master.GaveUp.items())):
                sys.stderr.write(f"{path}: {error}\n")

        print("Files:                %d" % (master.NFiles,))
        print("Directories found:    %d" % (master.NToScan,))
        print("Directories scanned:  %d" % (master.NScanned,))
        print("Directories:          %d" % (master.NDirectories,))
        print("  empty directories:  %d" % (len(master.EmptyDirs,)))
        print("Failed directories:   %d" % (len(master.GaveUp),))
        if include_sizes:
            print("Total size:           %.3f GB" % (master.TotalSize))
        t1 = time.time()
        elapsed = int(t1 - t0)
        s = elapsed % 60
        m = elapsed // 60
        print("Elapsed time:         %dm %02ds\n" % (m, s))
        
        if (not ignore_failed_directories) and master.GaveUp:
            failed = True

        total_size = None if failed else master.TotalSize/GB

        root_stats.update({
            "root_failed": master.RootFailed,
            "error": master.Error,
            "failed_subdirectories": master.GaveUp,
            "files": master.NFiles,
            "directories": master.NDirectories,
            "empty_directories":len(master.EmptyDirs),
            "end_time":t1,
            "elapsed_time": t1-t0,
            "total_size_gb": total_size
        })

        root_failed = master.RootFailed
            
    del my_stats["scanning"]
    my_stats["roots"].append(root_stats)
    if stats is not None:
        stats[stats_key] = my_stats
    if failed:
        stats["error"] = root_stats.get("error")
    return failed, root_failed
    
if __name__ == "__main__":
    import getopt, sys, time

    t0 = time.time()    
    opts, args = getopt.getopt(sys.argv[1:], "t:m:o:R:n:c:vqM:s:S:zd:kx")
    opts = dict(opts)
    
    if len(args) != 1 or not "-c" in opts:
        print(Usage)
        sys.exit(2)

    rse = args[0]
    config = Config(opts.get("-c"))

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
        nparts = config.nparts(rse)

    if nparts > 1:
        if not "-o" in opts:
            print ("Output prefix is required for partitioned output")
            print (Usage)
            sys.exit(2)

    output = opts.get("-o","out.list")

    out_list = PartitionedList.create(nparts, output, zout)

    dir_output = opts.get("-d")
    dir_list = PartitionedList.create(nparts, dir_output, zout) if dir_output else None

    server = config.scanner_server(rse)
    server_root = config.scanner_server_root(rse)
    include_sizes = config.scanner_include_sizes(rse) and not "-x" in opts
    purge_empty_dirs = config.scanner_param(rse, "purge_empty_dirs", default=False)
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
        "status":   "started"
    }
    
    if stats is not None:
        stats[stats_key] = my_stats
    
    failed = False
    all_roots_failed = True
    for root in config.scanner_roots(rse):
        try:
            failed, root_failed = scan_root(rse, root, config, my_stats, stats, stats_key, override_recursive_threshold, 
                    override_max_scanners, out_list, dir_list,
                    purge_empty_dirs, ignore_directory_scan_errors, include_sizes)
            all_roots_failed = all_roots_failed and root_failed
        except:
            exc = traceback.format_exc()
            print(exc)
            lines = exc.split("\n")
            scanning = my_stats.setdefault("scanning", {"root":root})
            scanning["exception"] = lines
            scanning["exception_time"] = time.time()
            failed = True
    
        if failed:
            break
           
    out_list.close()

    if failed or all_roots_failed:
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
 
    
        
