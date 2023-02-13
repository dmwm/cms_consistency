from pythreader import synchronized, ShellCommand, Primitive
import re, json, os, os.path, traceback
import subprocess, time, random, gzip
from py3 import to_str

def canonic_path(path):
    while path and "//" in path:
        path = path.replace("//", "/")
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    return path
    
class XRootDClient(Primitive):

    def __init__(self, server, is_redirector, server_root, timeout=300, name=None):
        Primitive.__init__(self, name=name)
        self.Timeout = timeout
        self.Server = server 
        self.ServerRoot = canonic_path(server_root)
        self.Servers = [server]
        self.IsRedirector = is_redirector
        
    def prescan(self, root):
        if self.IsRedirector:
            self.Servers = self.get_underlying_servers(self.Server, root, self.Timeout)

    def absolute_path(self, path):
        path = canonic_path(path)
        return canonic_path(path if path.startswith(self.ServerRoot) else self.ServerRoot + "/" + path)
        
    @synchronized
    def next_server(self):
        if len(self.Servers) > 1:
            server = self.Servers.pop(0)
            self.Servers.append(server)
        else:
            server = self.Servers[0]
        return server

    @synchronized
    def release_server(self, server):
        if len(self.Servers) > 1:
            i = self.Servers.index(server)
            if i > 0:
                self.Servers.pop(i)
                self.Servers.insert(i-1, server)

    Line_Patterns = [
        # UNIX FS ls -l style
        r"""
                (?P<mask>[drwx-]{10})\s+
                \S+\s+
                \S+\s+
                (?P<size>\d+)\s+
                \d{4}-\d{2}-\d{2}\s+
                \d{2}:\d{2}:\d{2}\s+
                (?P<path>[^ ]+)
        """,
        # xrdfs ls -l style
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
        
    HostPortRE = re.compile(r"[a-zA-Z-]+(\.[a-zA-Z0-9-]+)*(\:[0-9]+)?")

    def get_underlying_servers(self, redirector, location, timeout):
        # location is relative to site root
        # Query a redirector and return a single registered data server
        # On failure, return the original server address

        servers = [redirector]
        absolute_location = self.absolute_path(location)

        retcode, out, err = ShellCommand.execute(
                f"xrdfs {redirector} locate -m {absolute_location}", 
                timeout=timeout
        )

        if retcode == 0:
            lst = [x.split()[0] for x in out.split("\n") if " server " in x.lower() and "read" in x.lower()]
            lst = [x for x in lst if x and self.HostPortRE.match(x)]
            if lst:
                servers = lst
        return servers
        
    def rmdir(self, path):
        server = self.next_server()
        path = self.absolute_path(path)
        rmcommand = "xrdfs %s rmdir %s" % (server, path)
        reason = None
        try:    
            #print(f"would delete directory {self.Location} with {rmcommand}")
            retcode, out, err = ShellCommand.execute(rmcommand, timeout=self.Timeout)
            if retcode == 0:
                status = "OK"
            else:
                status = "failed"
                reason = err or out
        except RuntimeError:
            status = "timeout"
            reason = f"timeout ({self.Timeout})"
        except Exception as e:
            status = "failed"
            reason = str(e)
        return status, reason

    def stat(self, path):
        path = self.absolute_path(path)
        server = self.next_server()
        command = "xrdfs %s stat %s" % (server, path)
        try:    retcode, out, err = ShellCommand.execute(command, timeout=self.Timeout)
        except RuntimeError:
            return "timeout", None, None, None
        size = None
        typ = None
        for line in out.split("\n"):
            line = line.strip().lower()
            if line.startswith("flags:"):
                if "isdir" in line:
                    typ = "d"
                else:
                    typ = "f"
            elif line.startswith("size:"):
                try:
                    size = int(line.split(None, 1)[1])
                except:
                    size = None
        return "OK", None, typ, size

    def ls(self, location, recursive, with_meta, timeout=None):
        # returns list of paths relative to the server root, relative paths do start with "/"
        #print(f"scan({location}, rec={recursive}, with_meta={with_meta}):...")
        files = []
        dirs = []
        status = "OK"
        reason = ""
        timeout = timeout or self.Timeout

        location = self.absolute_path(location)
        server = self.next_server()
        lscommand = "xrdfs %s ls %s %s %s" % (server, "-l" if with_meta else "", "-R" if recursive else "", location)

        try:
            #print(f"lscommand: {lscommand}")
            retcode, out, err = ShellCommand.execute(lscommand, timeout=timeout)
            #print(f"retcode: {retcode}")
        except RuntimeError:
            status = "failed"
            reason = f"timeout ({self.Timeout})"
        else:
            if retcode:
                status = "failed"
                reason = "ls status code: %s, stderr: [%s]" % (retcode, err)

                status, reasoon, typ, size = self.stat(location)
                if status != "OK":
                    reason = "stat failed: " + (reason or "")
                else:
                    if typ == 'f':
                        status = "OK"
                        reason = ""
                        files = [(location, size)]
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
                    path = canonic_path(path)
                    assert self.ServerRoot == '/' or path.startswith(self.ServerRoot + "/"), f"Parsed path {path} is expected to start with server root {self.ServerRoot}"
                    if self.ServerRoot != '/':
                        path = path[len(self.ServerRoot):]
                    if is_file:
                        files.append((path, size))
                    else:
                        dirs.append((path, size))
        finally:
            self.release_server(server)

        return status, reason, dirs, files
        
if __name__ == "__main__":
    # test
    import sys, getopt
    Usage = """
    python xrootd_client <server> <root> [-t <timeout>] [-l] [-R] <path>
    """
    opts, args = getopt.getopt(sys.argv[1:], "lRt:")
    if len(args) < 2:
        print(Usage)
        sys.exit(2)
    opts = dict(opts)
    timeout = int(opts.get("-t", 30))

    client = XRootDClient(args[0], True, args[1], timeout=timeout)
    status, reason, dirs, files = client.ls(args[2], "-R" in opts, "-l" in opts)
    print(status, reason)
    if status == "OK":
        print("Files:   ", len(files))
        print("Dirs:    ", len(dirs))
        for path in dirs:
            print("d", path)
        for path in files:
            print("f", path)
    
    