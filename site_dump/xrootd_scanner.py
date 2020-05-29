from pythreader import TaskQueue, Task, Queue, PyThread
import re
from partition import part

from pythreader import ShellCommand

def runCommand(cmd, timeout=None, debug=None):
    if timeout is not None and timeout < 0: timeout = None
    if debug:
        debug("runCommand: %s" % (cmd,))
    cmd = ShellCommand(cmd)
    status, out, err = cmd.waitCompletion(timeout)
    if debug:
        debug("%s [%s] [%s]" % (status, out, err))
        
    if not out: out = err
    
    if status is None:
        cmd.kill()
        out = (out or "") + "\n subprocess timed out\n"
        status = 100
    
    return status, out


class Scanner(Task):
    
    def __init__(self, master, server, location, timeout):
        self.Server = server
        self.Master = master
        self.Location = location
        self.Timeout = timeout
        
    DirectoryRE = re.compile("^d")
    FileRE = re.compile("^-")
    PathRE = re.compile("[^ ]+$")
    SizeRE = re.compile("^[a-z-]+\s+[0-9-]+\s+\d\d:\d\d:\d\d\s*(?P<size>\d+)")

    def run(self):
        location = self.Location
        lscommand = "xrdfs %s ls -l %s" % (self.Server, self.Location)
        status, out = runCommand(lscommand, timeout, debug)
        if status:
            self.Master.scanner_failed(self, out)
        else:
            files = []
            lines = [x.strip() for x in out.split("\n")]
            for l in lines:
                l = l.strip()
                if l:
                    if self.FileRE.match(l):
                        #size = int(SizeRE.search(l).group("size"))
                        path = PathRE.search(l).group()
                        name = path.rsplit("/",1)[-1]
                        path = path if path.startswith(location) else location + "/" + path
                        files.append(path)
                    elif self.DirectoryRE.match(l):
                        path = PathRE.search(l).group()
                        path = path if path.startswith(location) else location + "/" + path
                        self.Master.addDirectory(path)
            if files:
                self.Master.addFiles(files)

class ScannerMaster(PyThread):
    
    def __init__(self, server, root, max_scanners, timeout):
        PyThread.__init__(self)
        self.Server = server
        self.Root = root
        self.MaxScanners = max_scanners
        self.Results = DEQueue(10)
        self.ScannerQueue = TaskQueue(max_scanners)
        self.Timeout = timeout
        self.Done = False
        self.Error = None
        self.Failed = False

    def run(self):
        self.addDirectory(self.Root)
        self.ScannerQueue.waitUntilEmpty()
        self.Done = True
        
    def addFiles(self, files):
        self.Results.append(files)
        
    def addDirectory(self, path):
        if not self.Failed:
            self.ScannerQueue.addTask(
                Scanner(self, self.Server, path, self.Timeout)
            )
    
    def files(self):
        while not (self.Done and self.Results.isEmpty()):
            lst = self.Results.pop()
            for path in lst:
                yield path
    
    @synchronized
    def scanner_failed(self, error):
        self.Error = error
        self.Failed = True
        self.ScannerQueue.hold()
        self.ScannerQueue.flush()
            
Usage = """
python xrootd_scanner.py [options] <server> <root>
    -n <n>                   - partition the output into n parts. Default 1. If not 1, -o is required
    -o <output file prefix>  - output will be sent to <output>.00000, <output>.00001, ...
    -r <prefix-to-remove>    - remove prefix from paths
    -a <prefix-to-add>       - add prefix after removing the one specified with -p
    -m <max scanners>        - max number of directory scanners to run concurrenty (default:5)
    -t <timeout>             - xrdfs ls operation timeout (default 30 seconds)
"""
        
if __name__ == "__main__":
    import getopt, sys
    
    opts, args = getopt.getopt(sys.argv[1:], "t:m:r:a:n:o:")
    opts = dict(opts)
    
    if len(args) != 2:
        print(Usage)
        sys.exit(2)
        
    server, root = args
    max_scanners = int(opts.get("-m", 5))
    timeout = int(opts.get("-t", 30))
    remove_prefix = opts.get("-r")
    add_prefix = opts.get("-a")
    output = opts.get("-o")
    nparts = int(opts.get("-n", 1))
    if nparts > 1:
        if not output:
            print ("Output prefix is required for partitioned output")
            print (Usage)
            sys.exit(2)
    
    if not output:
        outputs = [sys.stdout]
    else:
        outputs = [open("%s.%05d" % (output, i), "w") for i in range(nparts)]
            
    master = ScannerMaster(server, top, max_scanners, timeout)
    master.start()
    for path in master.files():
        if remove_prefix and path.startswith(remove_prefix):
            path = path[len(remove_prefix):]
        if add_prefix:
            path = add_prefix + path
        i = 0 if nparts == 1 else part(nparts, path)
        outputs[i].write("%s\n" % (path,))

    [out.close() for out in outputs]
    
        