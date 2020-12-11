from webpie import WPApp, WPHandler
import sys, glob, json, time

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
        
    def list_rses(self):
        files = glob.glob(f"{self.Path}/*_stats.json")
        rses = set()
        for path in files:
            fn = path.rsplit("/",1)[-1]
            rse, timestamp, typ, ext = self.parse_filename(fn)
            rses.add(rse)
        return sorted(list(rses))
    
    def list_runs(self, rse):
        files = glob.glob(f"{self.Path}/{rse}_*_stats.json")
        runs = []
        for path in files:
            fn = path.split("/",1)[-1]
            rse, timestamp, typ, ext = self.parse_filename(fn)
            runs.append(timestamp)
        return sorted(runs)
    
    def get_data(self, rse, run, typ):
        ext = "json" if typ == "stats" else "list"
        path = f"{self.Path}/{rse}_{run}_{typ}.{ext}"
        with open(path, "r") as f:
            if typ == "stats":
                return json.loads(f.read())
            else:
                return [l.strip() for l in f.readlines() if l.strip()]
                
    def get_run(self, rse, run):
        dark = self.get_data(rse, run, "D")
        missing = self.get_data(rse, run, "M")
        stats = self.get_data(rse, run, "stats")
        
        out = {
            "stats":stats,
            "dark":dark,
            "missing":missing
        }
        #print(out)
        return out

class Handler(WPHandler):
    
    def index(self, request, relpath, **args):
        #
        # list available RSEs
        #
        rses = self.App.DataViewer.list_rses()
        #print(rses)
        return self.render_to_response("rses.html", rses=rses)
        
    def show_rse(self, request, relpath, rse=None, **args):
        runs = self.App.DataViewer.list_runs(rse)
        runs = sorted(runs, reverse=True)
        runs_with_stats = [(run, self.App.DataViewer.get_run(rse, run)["stats"]) for run in runs]
        #print("runs_with_stats:", runs_with_stats)
        return self.render_to_response("show_rse.html", rse=rse, runs_with_stats=runs_with_stats)

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
            
            

    def show_run(self, request, relpath, rse=None, run=None, **args):
        run_info = self.App.DataViewer.get_run(rse, run)
        stats = run_info["stats"]
        stats_parts = [(part, stats[part]) for part in ["dbdump_before", "scanner", "dbdump_after", "cmp3"]]
        return self.render_to_response("show_run.html", rse=rse, run=run,
            dbdump_before=stats["dbdump_before"],
            dbdump_after=stats["dbdump_after"],
            scanner=stats["scanner"],
            scanner_roots = sorted(stats["scanner"]["roots"], key=lambda x:x["root"]),
            cmp3=stats["cmp3"],
            stats=stats_parts,
            dark=self.common_paths(sorted(run_info["dark"])),
            missing = self.common_paths(sorted(run_info["missing"])),
            stats_parts=stats_parts
        )

def as_dt(t):
    return time.ctime(t)
    
def as_json(d):
    return "\n"+json.dumps(d, indent=4)
    
def hms(t):
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
    

class App(WPApp):
    
    def __init__(self, handler, path):
        WPApp.__init__(self, handler)
        self.DataViewer = DataViewer(path)

    def init(self):
        import os
        home = os.path.dirname(__file__) or "."
        self.initJinjaEnvironment(tempdirs=[home], 
            filters={"hms":hms , "as_dt":as_dt, "as_json":as_json})
        
        
Usage = """
python server.py <port> <data path>
"""

if __name__ == "__main__":
    import sys, getopt

    if not sys.argv[1:]:
        print (Usage)
        sys.exit(2) 
    
    port = int(sys.argv[1])
    path = sys.argv[2]
    
    App(Handler, path).run_server(port)

        
        
        
        
        
