from pythreader import TaskQueue, Task, DEQueue, PyThread, synchronized
import re
import subprocess, time
from partition import part

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
        

def runCommand(cmd, timeout=None, debug=None):
    return p.returncode, out

class Killer(PyThread):

    def __init__(self, scanner, timeout):
        PyThread.__init__(self)
        self.Scanner = scanner
        self.Timeout = timeout
        self.Stop = False

    def stop(self):
        self.Stop = True

    def run(self):
        t1 = time.time() + self.Timeout
        while not self.Stop and time.time() < t1:
                time.sleep(0.5)
        if not self.Stop:
                self.Scanner.killme()
                self.Scanner = None
    
class Scanner(Task):
    
    def __init__(self, master, server, location, recursive, timeout):
        Task.__init__(self)
        self.Server = server
        self.Master = master
        self.Location = location
        self.Timeout = timeout
        self.Recursive = recursive
        self.Subprocess = None
        self.Done = False
        self.Killed = False
        self.Started = None
        self.Elapsed = None

    def __str__(self):
        return "Scanner(%s)" % (self.Location,)

    def message(self, status, stats):
        self.Master.message("%-8s %-22s %s" % (status, stats, truncated_path(self.Master.Root, self.Location)))
        

    @synchronized
    def killme(self):
        if self.Subprocess is not None:
                self.Killed = True
                self.Subprocess.terminate()
                
    def run(self):
        self.Started = t0 = time.time()
        location = self.Location
        self.Recursive = recursive = self.Recursive and not self.Master.recursionVeto(location)
        stats = "r" if self.Recursive else " "
        self.message("start", stats)
        
        # make sure to update self.Recursive too so the Master knows how this was scanned

        lscommand = "xrdfs %s ls %s %s" % (self.Server, "-R" if recursive else "", self.Location)

        killer = Killer(self, self.Timeout)

        with self:
                # the killer process will wait for self.Subprocess to become not None or Done to become True
                self.Subprocess = subprocess.Popen(lscommand, shell=True, 
                        stderr=subprocess.PIPE,
                        stdout=subprocess.PIPE)
                killer.start()          # do not start killer until self.Subprocess is set

        out, err = self.Subprocess.communicate()

        with self:
                # make this a critical section so the killer process does not intercept us
                killer.stop()
                retcode = self.Subprocess.returncode
                self.Subprocess = None
                
        if retcode or self.Killed:
            self.Elapsed = time.time() - self.Started
            if self.Killed:
		    stats = "%1s %5.1fs" % ("r" if self.Recursive else " ", self.Elapsed)
                    status = "timeout"
            else:
		    stats = "%1s %5.1fs %d" % ("r" if self.Recursive else " ", self.Elapsed, retcode)
                    status = "failed"
            self.message(status, stats)
            self.Master.scanner_failed(self, err)
            return

        lines = [x.strip() for x in out.split("\n")]
        
        for l in lines:
            if l and not l.startswith(self.Location):
                self.Elapsed = time.time() - self.Started
                self.Master.scanner_failed(self, "Invalid line in output: %s" % (l,))
                return
                
        files = []
        dirs = []
        for l in lines:
            l = l.strip()
            if l:
                last_word = l.rsplit("/",1)[-1]
                if '.' in last_word:
                    path = l
                    path = path if path.startswith(location) else location + "/" + path
                    if not path.endswith("/."):
                            files.append(path)
                else:
                    path = l
                    path = path if path.startswith(location) else location + "/" + path
                    dirs.append(path)
        self.Elapsed = time.time() - self.Started
        stats = "%1s %5.1fs %6d/%-3d" % ("r" if recursive else " ", self.Elapsed, len(files), len(dirs))
        self.message("done", stats)
        if not files and not dirs:
            self.Master.scanner_empty(self)
        else:
            self.Master.scanner_succeeded(self, files, dirs)


class ScannerMaster(PyThread):
    
    MAX_RECURSION_FAILED_COUNT = 5
    MAX_ERRORS = 5
    REPORT_INTERVAL = 10.0
    
    def __init__(self, server, root, recursive_threshold, max_scanners, timeout, quiet, display_progress):
        PyThread.__init__(self)
        self.RecursiveThreshold = recursive_threshold
        self.Server = server
        self.Root = self.canonic(root)
        self.MaxScanners = max_scanners
        self.Results = DEQueue(100)
        self.ScannerQueue = TaskQueue(max_scanners)
        self.Timeout = timeout
        self.Done = False
        self.Error = None
        self.Failed = False
        self.Directories = set()
        self.RecursiveFailed = {}       # parent path -> count
        self.Errors = {}                # location -> count
        self.GaveUp = set()
        self.LastReport = time.time()
        self.EmptyDirs = set()
        self.NScanned = 0
        self.Quiet = quiet
        self.DisplayProgress = display_progress and Use_tqdm and not quiet
        if self.DisplayProgress:
            self.TQ = tqdm.tqdm(total=1, unit="dir")
            self.LastV = 0
        self.NFiles = 0
            
        
    def run(self):
        self.addDirectory(self.Root, True)
        self.ScannerQueue.waitUntilEmpty()
        self.Results.close()
        self.Done = True
        
    def addFiles(self, files):
        if not self.Failed:
            self.Results.append(files)
            self.NFiles += len(files)

    def canonic(self, path):
        while path and "//" in path:
                path = path.replace("//", "/")
        return path
        
    def parent(self, path):
        parts = path.rsplit("/", 1)
        if len(parts) < 2:
            return "/"
        else:
            return parts[0]
            
    @synchronized
    def recursionVeto(self, path):
        # check if "many" subdirectories under this path's parent fail to scan recursively
        # if so, tell the scanner not to bother and run in non-recursive way
        parent = self.parent(path)
        n = self.RecursiveFailed.get(parent, 0)
        return n >= self.MAX_RECURSION_FAILED_COUNT
      
    def addDirectory(self, path, scan):
        if not self.Failed:
            path = self.canonic(path)
            self.Directories.add(path)
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
		    self.ScannerQueue.addTask(
			Scanner(self, self.Server, path, recursive, self.Timeout)
		    )
        
    def addDirectories(self, dirs, scan=True):
        for d in dirs:
            self.addDirectory(d, scan)
        self.show_progress()
        self.report()
            
    @synchronized
    def report(self):
        if time.time() > self.LastReport + self.REPORT_INTERVAL:
            waiting, active = self.ScannerQueue.tasks()
            #sys.stderr.write("--- Locations to scan: %d\n" % (len(active)+len(waiting),))
            self.LastReport = time.time()

    def scanner_failed(self, scanner, error):
        path = scanner.Location
        if scanner.Recursive:
            with self:
                parent = self.parent(path)
                nfailed = self.RecursiveFailed.get(parent, 0)
                self.RecursiveFailed[parent] = nfailed + 1
                
        retry = True
        if not scanner.Recursive:
            with self:
                nerrors = self.Errors.setdefault(path, 0)
                nerrors = self.Errors[path] = nerrors + 1
                retry = nerrors < self.MAX_ERRORS
        if retry:
            self.ScannerQueue.addTask(
                Scanner(self, self.Server, path, False, self.Timeout)
            )
        else:
            self.GaveUp.add(path)
            self.NScanned += 1  
            #sys.stderr.write("Gave up on: %s\n" % (path,))
        self.show_progress()		#"Error scanning %s: %s -- retrying" % (scanner.Location, error))
        
    def scanner_empty(self, scanner):
        path = scanner.Location
        rescan = False
        with self:
              if not path in self.EmptyDirs:
                # rescan again, once
                self.EmptyDirs.add(path)
                #rescan = True
        if rescan:
            self.ScannerQueue.addTask(
                Scanner(self, self.Server, path, False, self.Timeout)
            )
        else:
            self.NScanned += 1  
        self.show_progress()            
            
    def scanner_succeeded(self, scanner, files, dirs):
        location = scanner.Location
        with self:
            if location in self.EmptyDirs:
                self.EmptyDirs.remove(location)
            if scanner.Recursive:
                parent = self.parent(location)
                nfailed = self.RecursiveFailed.get(parent, 0)
                self.RecursiveFailed[parent] = nfailed - 1      
        self.NScanned += 1  
        self.addFiles(files)
        self.addDirectories(dirs, not scanner.Recursive)
        self.show_progress()

    def files(self):
        while not (self.Done and len(self.Results) == 0):
            lst = self.Results.pop()
            if lst:
                    for path in lst:
                        yield self.canonic(path)
                        
    @synchronized
    def show_progress(self, message=None):
        if self.DisplayProgress:
            self.TQ.total = max(1, len(self.Directories))
            delta = max(0, self.NScanned - self.LastV)
            self.TQ.update(delta)
            self.LastV = self.NScanned
            self.TQ.set_postfix(f=self.NFiles, d=len(self.Directories), e=len(self.EmptyDirs))
            if message:
                self.TQ.write(message)   
                
    @synchronized
    def message(self, message):
        if not self.Quiet:
		if self.DisplayProgress:
		    self.TQ.write(message)
		else:
		    print(message)

    def close_progress(self):
        if self.DisplayProgress:
            self.TQ.close()
                
             
            
Usage = """
python xrootd_scanner.py [options] <rse>
    Options:
    -c <config.json>            - config file, required
    -o <output file prefix>     - output will be sent to <output>.00000, <output>.00001, ...
    -t <timeout>                - xrdfs ls operation timeout (default 30 seconds)
    -m <max workers>            - default 5
    -R <recursion depth>        - start using -R at or below this depth (dfault 3)
    -n <nparts>
    -d                          - display progress
    -q                          - quiet - only print summary
"""
        
if __name__ == "__main__":
    import getopt, sys, time

    t0 = time.time()    
    opts, args = getopt.getopt(sys.argv[1:], "t:m:o:R:n:c:dq")
    opts = dict(opts)
    
    if len(args) != 1 or not "-c" in opts:
        print(Usage)
        sys.exit(2)

    rse = args[0]
    config = Config(opts.get("-c"))

    root = config.scanner_root(rse)
    server = config.scanner_server(rse)
    remove_prefix = config.scanner_remove_prefix(rse) or root
    add_prefix = config.scanner_add_prefix(rse)
    max_scanners = config.scanner_workers(rse) or int(opts.get("-m", 5))
    recursive_threshold = config.scanner_recursion_threshold(rse) or int(opts.get("-R", 3))
    timeout = config.scanner_timeout(rse) or int(opts.get("-t", 30))
    quiet = "-q" in opts
    display_progress = not quiet and "-d" in opts
    if "-n" in opts:
        nparts = int(opts["-n"])
    else:
        nparts = config.nparts(rse)

    output = opts.get("-o")
    if nparts > 1:
        if not output:
            print ("Output prefix is required for partitioned output")
            print (Usage)
            sys.exit(2)
    
    if not output:
        outputs = [sys.stdout]
    else:
        outputs = [open("%s.%05d" % (output, i), "w") for i in range(nparts)]
       
    t0 = time.time()
 
    master = ScannerMaster(server, root, recursive_threshold, max_scanners, timeout, quiet, display_progress)
    master.start()
    n = 0
    for path in master.files():

        if remove_prefix and path.startswith(remove_prefix):
            path = path[len(remove_prefix):]
            
        if add_prefix:
            path = add_prefix + path
            
        i = 0 if nparts == 1 else part(nparts, path)
        outputs[i].write("%s\n" % (path,))

        n += 1
        if False and (n % 100 == 0):
            scanners = list(master.ScannerQueue.activeTasks())
            print ("[Active scanners: %d]" % (len(scanners),))
            for s in scanners:
                print "    %s" % (s,)

    if display_progress:
        master.close_progress()

    if master.Failed:
        print("Scanner failed:", master.Error)
        
    if master.GaveUp:
        print("Scanner failed to scan the following %d locations:" % (len(master.GaveUp),))
        for p in sorted(list(master.GaveUp)):
            print(p)

    [out.close() for out in outputs]
    t = int(time.time() - t0)
    print("Files:                %d" % (n,))
    print("Directories:          %d" % (len(master.Directories,)))
    print("  empty directories:  %d" % (len(master.EmptyDirs,)))
    print("Failed directories:   %d" % (len(master.GaveUp),))
    #print("Directories:")
    #for d in sorted(list(master.Directories)):
    #    print(d)
    t = int(time.time() - t0)
    s = t % 60
    m = t // 60
    sys.stderr.write("Elapsed time:         %dm %02ds\n" % (m, s))
    if master.GaveUp:
        sys.exit(1)
    else:
        sys.exit(0)
 
    
        
