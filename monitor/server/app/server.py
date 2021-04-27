from webpie import WPApp, WPHandler
import sys, glob, json, time, os
from datetime import datetime
from wm_handler import WMHandler, WMDataSource

Version = "1.2c"

class DataViewer(object):
    
    def __init__(self, path):
        self.Path = path
        
    def parse_filename(self, fn):
        # filename looks like this:
        #
        #   <rse>_%Y_%m_%d_%H_%M_<type>.<extension>
        #
        fn, ext = fn.rsplit(".",1)
        parts = fn.split("_")
        typ = parts[-1]
        timestamp_parts = parts[-6:-1]
        timestamp = "_".join(timestamp_parts)
        rse = "_".join(parts[:-6])
        return rse, timestamp, typ, ext
        
    def is_mounted(self):
        return os.path.isdir(self.Path)

    def status(self):
        if not os.path.isdir(self.Path):
            return "Data volume %s does not exist" % (self.Path,)
        return "OK"
        
    def list_rses(self):
        files = glob.glob(f"{self.Path}/*_stats.json")
        rses = set()
        for path in files:
            fn = path.rsplit("/",1)[-1]
            rse, timestamp, typ, ext = self.parse_filename(fn)
            rses.add(rse)
        return sorted(list(rses))
        
    NLAST_RUNS = 8
    
    def list_runs(self, rse, nlast=NLAST_RUNS):
        files = glob.glob(f"{self.Path}/{rse}_*_stats.json")
        runs = []
        for path in files:
            fn = path.split("/",1)[-1]
            rse, timestamp, typ, ext = self.parse_filename(fn)
            runs.append(timestamp)
        return sorted(runs)[-nlast:]
        
    def file_path(self, rse, run, typ):
        ext = "json" if typ == "stats" else "list"
        return f"{self.Path}/{rse}_{run}_{typ}.{ext}"
        
    def raw_stats(self, rse, run):
        # returns unparsed JSON text
        path = self.file_path(rse, run, "stats")
        return open(path, "r").read(), os.path.getmtime(path)
        
    def get_data(self, rse, run, typ, limit=None):
        ext = "json" if typ == "stats" else "list"
        path = f"{self.Path}/{rse}_{run}_{typ}.{ext}"
        #print("get_data: path:", path)
        try:
            f = open(path, "r")
        except:
            #print("get_data: error ")
            return None
        if typ == "stats":
            stats = json.loads(f.read())
            if "scanner" in stats:
                scanner_stats = stats["scanner"]
                if not "total_files" in scanner_stats:
                    nfiles = ndirectories = 0
                    for root_info in scanner_stats["roots"]:
                        nfiles += root_info["files"]
                        ndirectories += root_info["directories"]
                    scanner_stats["total_files"] = nfiles
                    scanner_stats["total_directories"] = ndirectories
            out = stats
        else:
            out = []
            while limit is None or len(out) < limit:
                l = f.readline()
                if not l:
                    break
                l = l.strip()
                if l:
                    out.append(l)
            return out
        f.close()
        return out
                
    def get_stats(self, rse, run):
        stats = self.get_data(rse, run, "stats")
        ndark = nmissing = None
        if "cmp3" in stats:
            ndark = stats["cmp3"].get("dark")
            nmissing = stats["cmp3"].get("missing")
        for k in ["dbdump_before", "scanner", "dbdump_after", "cmp3"]:
            d = stats.get(k)
            if isinstance(d, dict) and not "elapsed" in d:
                d["elapsed"] = None
                if d.get("end_time") is not None and d.get("start_time") is not None:
                    d["elapsed"] = d["end_time"] - d["start_time"]
        return stats, ndark, nmissing
        

    def get_dark_or_missing(self, rse, run, typ, limit):
        path = f"{self.Path}/{rse}_{run}_{typ}.list"
        path_gz = path + ".gz"
        try:
            f = gzip.open(path_gz, "rt")
        except:
            f = open(path, "r")
        while limit is None or limit > 0:
            l = f.readline()
            if not l:
                break
            l = l.strip()
            if l:
                yield l
                if limit is not None:
                    limit -= 1        
        f.close()

    def get_dark(self, rse, run, limit=None):
        return self.get_dark_or_missing(rse, run, "D", limit)

    def get_missing(self, rse, run, limit=None):
        return self.get_dark_or_missing(rse, run, "M", limit)
        
    def last_stats(self, rse):
        last_run = self.list_runs(rse, 1)
        if last_run:
            last_run = last_run[0]
            return self.get_stats(rse, last_run)
        else:
            return None
        

def display_file_list(lst):
    Indent = "    "
    last_items = []
    out = []
    for path in lst:
        items = [item for item in path.split("/") if item]
        n_common = 0
        for li, i in zip(items, last_items):
            if li == i:
                n_common += 1
            else:
                break
        tail = items[n_common:]
        indent = Indent * n_common
        for i, item in enumerate(tail):
            if i < len(tail)-1:
                item += "/"
            if not indent: item = "/" + item
            out.append(indent + item)
            indent += Indent
        last_items = items
    return out
    

class Handler(WPHandler):
    
    def __init__(self, *params, **args):
        WPHandler.__init__(self, *params, **args)
        self.WM = WMHandler(*params, **args)
        
    COMPONENTS = ["dbdump_before", "scanner", "dbdump_after", "cmp3"]

    def run_summary(self, stats):
        status = None
        tstart, tend = None, None
        failed_comp = None
        running_comp = None
        all_done = True
        for comp in self.COMPONENTS:
            if comp in stats:
                comp_stats = stats[comp]
                comp_status = comp_stats.get("status")
                tend = comp_stats.get("end_time")
                comp_started = comp_status == "started" or comp_stats.get("start_time") is not None
                comp_done = comp_status == "done" or comp_stats.get("end_time") is not None
                comp_running = comp_started and not (comp_status in ("done", "failed"))
                if not comp_done:
                    all_done = False
                if "start_time" in comp_stats and tstart is None:
                    tstart = comp_stats["start_time"]
                if comp_started and status is None:
                    status = "started"
                if comp_running:
                    running_comp = comp
                if comp_status == "failed":
                    status = "failed"
                    failed_comp = comp
                    break
            else:
                all_done = False
        
        last_comp = stats.get(self.COMPONENTS[-1])
        if last_comp:
            if last_comp.get("status") == "done":
                all_done = True
        
        if all_done:
            status = "done"
        else:
            tend = None
            
        return {
            "status": status,
            "start_time": tstart,
            "end_time": tend,
            "failed": failed_comp,
            "running": running_comp
        }
        
    
    def index(self, request, relpath, **args):
        #
        # list available RSEs
        #
        rses = self.App.DataViewer.list_rses()
        infos = []
        for i, rse in enumerate(rses):
            start_time, ndark, nmissing, nerrors, error = None, None, None, None, None
            try:
                #if i % 5 == 1:
                #    raise ValueError('debug "debug"')    
                stats, ndark, nmissing = self.App.DataViewer.last_stats(rse)
                summary = self.run_summary(stats)
                
            except Exception as e:
                exc = str(e).replace('"', r'\"')
                error = "Data parsing error: %s" % (exc,)
            #print("index: stats:", info.get("stats"))
            infos.append((rse, summary, ndark, nmissing, error))
            #print("index:", rse, start_time, ndark, nmissing, nerrors)
            sys.stdout.flush()
            
        #print(infos)
        return self.render_to_response("rses.html", infos=infos)
        
    def probe(self, request, relpath, **args):
        return self.App.DataViewer.status(), "text/plain"
        return "OK" if self.App.DataViewer.is_mounted() else ("Data directory unreachable", 500)
        
    def raw_stats(self, request, relpath, rse=None, run=None, **args):
        runs = self.App.DataViewer.list_runs(rse)
        raw_stats = mtime = None
        if run:
            raw_stats, mtime = self.App.DataViewer.raw_stats(rse, run)
        return self.render_to_response("raw_stats.html", rse=rse, runs=runs, raw_stats=raw_stats, mtime=mtime)

    def show_rse(self, request, relpath, rse=None, **args):
        runs = self.App.DataViewer.list_runs(rse)
        runs = sorted(runs, reverse=True)
        runs_with_stats = [(run, self.App.DataViewer.get_stats(rse, run)) for run in runs]
        #print("runs_with_stats:", runs_with_stats)
        
        infos = []
        for run in runs:
            stats, ndark, nmissing = self.App.DataViewer.get_stats(rse, run)
            summary = self.run_summary(stats)
            start_time = summary["start_time"]
            status = summary["status"]
            if status == "failed":
                status += " " + summary["failed"]
            running = summary.get("running")
            infos.append((
                run, 
                {
                    "start_time":start_time, "ndark":ndark, "nmissing":nmissing, "status":status, "running":running
                }
            ))
        #print(infos)
        return self.render_to_response("show_rse.html", rse=rse, runs=infos)

    def common_paths(self, lst, space="&nbsp;"):
        lst = sorted(lst)
        prev = []
        out = []
        for path in lst:
            parts = path.split("/")
            i = 0
            for j, (x, y) in enumerate(zip(prev, parts)):
                if x == y:
                    i = j
                else:
                    break
            head = "/".join(parts[:i+1])
            tail = "/".join(parts[i+1:])
            out.append(space*len(head)+"/"+tail)
            prev = parts
        return out
        
    def display_file_list(self, lst):
        Indent = "    "
        last_items = []
        out = []
        for path in sorted(lst or []):
            items = [item for item in path.split("/") if item]
            n_common = 0
            for li, i in zip(items, last_items):
                if li == i:
                    n_common += 1
                else:
                    break
            tail = items[n_common:]
            indent = Indent * n_common
            for i, item in enumerate(tail):
                if i < len(tail)-1:
                    item += "/"
                if not indent: item = "/" + item
                out.append(indent + item)
                indent += Indent
            last_items = items
        return out
    
    def check_run(self, stats):
        errors = []
        if not "scanner" in stats or stats["scanner"] is None:
            errors.append("Site scanner statistics missing")
        if not "dbdump_before" in stats or stats["dbdump_before"] is None:
            errors.append("DB dump before site scan statistics missing")
        if not "dbdump_after" in stats or stats["dbdump_after"] is None:
            errors.append("DB dump after site scan statistics missing")
        if not "cmp3" in stats or stats["cmp3"] is None:
            errors.append("Comparison statistics missing")
        if run_info.get("dark") is None:
            errors.append("Dark file list not found")
        if run_info.get("missing") is None:
            errors.append("Missing file list not found")
        return errors
        
    def dark(self, request, relpath, rse=None, run=None, **args):
        lst = self.App.DataViewer.get_dark(rse, run)
        return (path+"\n" for path in lst), {
            "Content-Type":"text/plain",
            "Content-Disposition":"attachment"
        }
            
    def missing(self, request, relpath, rse=None, run=None, **args):
        lst = self.App.DataViewer.get_mssing(rse, run)
        return (path+"\n" for path in lst), {
            "Content-Type":"text/plain",
            "Content-Disposition":"attachment"
        }
    
    LIMIT = 1000
    
    def show_run(self, request, relpath, rse=None, run=None, **args):
        stats, ndark, nmissing = self.App.DataViewer.get_stats(rse, run)
        summary = self.run_summary(stats)
        errors = []
        if summary["status"] == "failed":
            failed_comp = summary["failed"]
            errors = ["%s failed:" % failed_comp]
            failed_stats = stats[failed_comp]
            if failed_stats.get("error"):
                errors.append("error: %s", failed_stats["error"])
                
        stats_parts = [(part, part_name, stats.get(part)) for part, part_name in 
            [
                ("dbdump_before", "DB dump before scan"),
                ("scanner", "Site scanner"),
                ("dbdump_after", "DB dump after scan"),
                ("cmp3", "Comparison")
            ]
            if stats.get(part)
        ]
        scanner_roots = []
        if "scanner" in stats and "roots" in stats["scanner"]:
            scanner_roots = sorted(stats["scanner"]["roots"], key=lambda x:x["root"])
        
        dark_truncated = (ndark or 0)  > self.LIMIT
        missing_truncated = (nmissing or 0) > self.LIMIT
        
        dark = self.App.DataViewer.get_dark(rse, run, self.LIMIT)
        missing = self.App.DataViewer.get_missing(rse, run, self.LIMIT)
        
        return self.render_to_response("show_run.html", 
            rse=rse, run=run,
            errors = errors,
            dark_truncated = dark_truncated, missing_truncated=missing_truncated,
            dbdump_before=stats.get("dbdump_before"),
            dbdump_after=stats.get("dbdump_after"),
            scanner=stats.get("scanner"),
            scanner_roots = scanner_roots,
            cmp3=stats.get("cmp3"),
            stats=stats,
            ndark = ndark, nmissing=nmissing,
            dark=self.display_file_list(dark),
            missing = self.display_file_list(missing),
            stats_parts=stats_parts,
            time_now = time.time()
        )

def as_dt(t):
    # datetim in UTC
    if t is None:
        return ""
    dt = datetime.utcfromtimestamp(t)
    return dt.strftime("%Y-%m-%d %H:%M:%S")
    
def as_JSON_Date(t):
    # datetim in UTC
    if t is None:
        return "null"
    dt = datetime.utcfromtimestamp(t)
    return dt.strftime("new Date(%d, %d, %d)" % (dt.year, dt.month, dt.day))
    
def as_json(d):
    return "\n"+json.dumps(d, indent=4)
    
def hms(t):
    
    if t is None:
        return ""
    if t < 100:
        return "%.2fs" % (t)
    
    t = int(t)
    s = t % 60
    t //= 60
    m = t % 60
    h = t // 60
    
    if h == 0:
        return f"{m}m{s}s"
    else:
        return f"{h}h{m}m"
        
def path_type(path):
    return "dir" if path.endswith("/") else "file"
    

class App(WPApp):

    Version = Version
    
    def __init__(self, handler, path, prefix, wm_path):
        WPApp.__init__(self, handler, prefix=prefix)
        self.DataViewer = DataViewer(path)
        self.WMDataSource = WMDataSource(wm_path)
        

    def init(self):
        import os
        home = os.path.dirname(__file__) or "."
        self.initJinjaEnvironment(tempdirs=[home], 
            filters={
                "hms":hms , "as_dt":as_dt, "as_json":as_json, "path_type":path_type,
                "as_JSON_Date":as_JSON_Date
            }
            )
        
        
Usage = """
python server.py [-r <url prefix to remove>] <port> <cc data path> <wm data path>
"""

if __name__ == "__main__":
    import sys, getopt

    #print("server.py: sys.argv:", sys.argv)

    opts, args = getopt.getopt(sys.argv[1:], "r:ld")
    opts = dict(opts)

    if not args:
        print (Usage)
        sys.exit(2) 
    
    port = int(args[0])
    cc_path, wm_path = args[1:]
    
    prefix = opts.get("-r")
    logging="-l" in opts
    debug=sys.stdout if "-d" in opts else None

    print("Starting server:\n  port %s\n  CC path %s\n  WM path %s" % (port, cc_path, wm_path))

    sys.stdout.flush()
    
    App(Handler, cc_path, prefix, wm_path).run_server(port, logging=logging, debug=debug)

        
        
        
        
        
