from webpie import WPApp, WPHandler, WPStaticHandler
import sys, glob, json, time, os, gzip
from datetime import datetime
from wm_handler import WMHandler, UMDataSource
from data_source import CCDataSource, UMDataSource

Version = "1.9.4"


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
        self.WM = self.unmerged = WMHandler(*params, **args)
        self.static = WPStaticHandler(*params, **args)
        
    def __index(self, request, relpath, **args):
        #
        # list available RSEs
        #
        data_source = self.App.CCDataSource

        for rse, stats in data_source.latest_stats_per_rse().items():
            summary = data_source.run_summary(stats)
            infos.append(
                {
                    "rse":      rse, 
                    "run":      stats["run"], 
                    "summary":  summary, 
                    "error":    stats["error"]
                }
            )
            #print("index:", rse, start_time, ndark, nmissing, nerrors)
            #sys.stdout.flush()
            
        #print(infos)
        return self.render_to_response("rses.html", infos=infos)
        
    def index_combined(self, request, relpath, sort="rse", **args):
        #
        # list available RSEs
        #
        cc_data_source = self.App.CCDataSource
        um_data_source = self.App.UMDataSource

        cc_stats = cc_data_source.latest_stats_per_rse()
        cc_summaries = {rse: cc_data_source.run_summary(stats) for rse, stats in cc_stats.items()}
        print("cc stats available for:", list(cc_stats.keys()))

        um_stats = um_data_source.latest_stats_per_rse()
        um_summaries = {rse: um_data_source.run_summary(stats) for rse, stats in um_stats.items()}
        
        #print("um_stats:")
        #for rse, stats in um_stats.items():
        #    print (rse, stats)
        #sys.stdout.flush()
        
        all_rses = set(cc_stats.keys()) | set(um_stats.keys())
        all_rses = sorted(list(all_rses))

        infos = [
            {
                "rse":        rse,
                "cc_summary":   cc_summaries.get(rse),
                "um_summary":   um_summaries.get(rse)
            } 
            for rse in all_rses
        ]
        
        if sort == "rse":
            infos = sorted(infos, key=lambda x: x["rse"])
        elif sort == "cc_run":
            infos = sorted(infos, key=lambda x: ((x["cc_summary"] or {}).get("start_time", -1), x["rse"]))
        elif sort == "-cc_run":
            infos = sorted(infos, key=lambda x: (-(x["cc_summary"] or {}).get("start_time", -1), x["rse"]))
        elif sort == "um_run":
            infos = sorted(infos, key=lambda x: ((x["um_summary"] or {}).get("start_time", -1), x["rse"]))
        elif sort == "-um_run":
            infos = sorted(infos, key=lambda x: (-(x["um_summary"] or {}).get("start_time", -1), x["rse"]))
        
        return self.render_to_response("rses_combined.html", infos=infos)
        
    index = index_combined
        
    def probe(self, request, relpath, **args):
        return self.App.CCDataSource.status(), "text/plain"
        return "OK" if self.App.CCDataSource.is_mounted() else ("Data directory unreachable", 500)
        
    def raw_stats(self, request, relpath, rse=None, run=None, **args):
        runs = self.App.CCDataSource.list_runs(rse)
        raw_stats = mtime = None
        if run:
            raw_stats, mtime = self.App.CCDataSource.raw_stats(rse, run)
        return self.render_to_response("raw_stats.html", rse=rse, runs=runs, raw_stats=raw_stats, mtime=mtime)

    def show_rse(self, request, relpath, rse=None, **args):
        data_source = self.App.CCDataSource
        runs = data_source.list_runs(rse)
        runs = sorted(runs, reverse=True)
        
        cc_infos = []
        for run in runs:
            stats, ndark, nmissing, confirmed_dark = data_source.get_stats(rse, run)
            summary = data_source.run_summary(stats)
            start_time = summary["start_time"]
            status = summary["status"]
            if status == "failed":
                status = summary["failed"] + " failed"
            running = summary.get("running")
            cc_infos.append((
                run, 
                {
                    "start_time":       start_time, "ndark":ndark, "nmissing":nmissing, "status":status, "running":running,

                    "confirmed_dark":   summary["dark_stats"]["confirmed"], 
                    "acted_dark":       summary["dark_stats"]["acted_on"], 
                    "dark_status":      summary["dark_stats"]["action_status"],
                    "dark_status_reason": summary["dark_stats"].get("aborted_reason", ""),
                    
                    "acted_missing":summary["missing_stats"]["acted_on"], 
                    "missing_status":summary["missing_stats"]["action_status"],
                    "missing_status_reason":    summary["missing_stats"].get("aborted_reason", ""),
                    "start_time_milliseconds":int(start_time*1000)
                }
            ))
        #print(infos)
        
        um_data_source = self.App.UMDataSource
        um_runs = um_data_source.all_stats_for_rse(rse)
        um_runs = [r for r in um_runs if "start_time" in r and "end_time" in r]
        um_runs = sorted(um_runs, key=lambda r: r["run"], reverse=True)
        
        try:
            for r in um_runs:
                r["elapsed_time_hours"] = r["start_time_milliseconds"] = None
                if r.get("start_time"):
                    r["start_time_milliseconds"] = int(r["start_time"]*1000)
                    if r.get("end_time"):
                        r["elapsed_time_hours"] = (r["end_time"] - r["start_time"])/3600
                r.setdefault("total_size_gb", None)
                
        except KeyError:
            raise ValueError(f"key error in: {r}")
        return self.render_to_response("show_rse.html", rse=rse, cc_runs=cc_infos, um_runs=um_runs)

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
        lst = self.App.CCDataSource.get_dark(rse, run)
        return (path+"\n" for path in lst), {
            "Content-Type":"text/plain",
            "Content-Disposition":"attachment"
        }
            
    def missing(self, request, relpath, rse=None, run=None, **args):
        lst = self.App.CCDataSource.get_missing(rse, run)
        return (path+"\n" for path in lst), {
            "Content-Type":"text/plain",
            "Content-Disposition":"attachment"
        }
    
    LIMIT = 1000
    
    def show_run(self, request, relpath, rse=None, run=None, **args):
        data_source = self.App.CCDataSource
        stats, ndark, nmissing, confirmed_dark = data_source.get_stats(rse, run)
        summary = data_source.run_summary(stats)
        errors = []
        if summary["status"] == "failed":
            failed_comp = summary["failed"]
            errors = ["%s failed" % failed_comp]
            failed_stats = stats[failed_comp]
            if failed_stats.get("error"):
                errors.append("error: %s", failed_stats["error"])
                
        stats_parts = [(part, part_name, stats.get(part)) for part, part_name in 
            [
                ("dbdump_before", "DB dump before scan"),
                ("scanner", "Site scanner"),
                ("dbdump_after", "DB dump after scan"),
                ("cmp3", "Comparison"),
                ("cmp2dark", "Dark confirmation"),
                ("cc_dark", "Dark action"),
                ("cc_miss", "Missing action")
            ]
            if stats.get(part)
        ]
        for _,_,s in stats_parts:
            if "status" in s:
                status = (s.get("status") or "").lower() or None
                s["status"] = status
        scanner_roots = []
        if "scanner" in stats and "roots" in stats["scanner"]:
            scanner_roots = sorted(stats["scanner"]["roots"], key=lambda x:x["root"])
        
        dark_truncated = (ndark or 0)  > self.LIMIT
        missing_truncated = (nmissing or 0) > self.LIMIT
        
        dark = self.App.CCDataSource.get_dark(rse, run, self.LIMIT)
        missing = self.App.CCDataSource.get_missing(rse, run, self.LIMIT)
        
        #
        # retrofit failed directories
        #
        scanner = stats.get("scanner")
        if scanner:
            for r in scanner.get("roots", []):
                failed = r.get("failed_subdirectories") or {}
                if isinstance(failed, list):
                    out = {}
                    for line in failed:
                        error = ""
                        path = line
                        parts = line.split(None, 1)
                        if len(parts) > 1:
                            path, error = parts
                        out[path] = error
                    failed = out
                r["failed_subdirectories"] = failed
                    
        return self.render_to_response("show_run.html", 
            rse=rse, run=run,
            errors = errors,
            dark_truncated = dark_truncated, 
            missing_truncated=missing_truncated,
            dbdump_before=stats.get("dbdump_before"),
            dbdump_after=stats.get("dbdump_after"),
            scanner=stats.get("scanner"),
            scanner_roots = scanner_roots,
            cmp3=stats.get("cmp3"),
            stats=stats, summary=summary,
            ndark = ndark, nmissing=nmissing,
            dark=self.display_file_list(dark),
            missing = self.display_file_list(missing),
            stats_parts=stats_parts,
            time_now = time.time()
        )

    def files(self, request, relpath, rse=None, type="*"):
        files = self.App.CCDataSource.files(rse, type)
        sizes = [os.path.getsize(path) for path in files]
        return [f"{f} {s}\n" for f, s in zip(files, sizes)], "text/plain"
        
    def file(self, request, relpath):
        f = self.App.CCDataSource.open_file(relpath)
        def read_file(f):
            data = f.read(10240)
            while data:
                yield data
                data = f.read(10240)
        return read_file(f), "text/plain"
        
    MAX_HISTORY = 10
        
    def status_history(self, request, relpath, rses=None, **args):
        um_data_source = self.App.UMDataSource
        cc_data_source = self.App.CCDataSource

        if rses is None:
            rses = set(um_data_source.list_rses()) | set(cc_data_source.list_rses())
        else:
            rses = rses.split(",")
            
        data = {}      # {rse -> (cc_total, um_total, cc_success, um_success)}
        
        for rse in rses:
            if not rse: continue
            um_summaries = [um_data_source.run_summary(x) for x in um_data_source.all_stats_for_rse(rse)]
            cc_summaries = [cc_data_source.run_summary(x) for x in cc_data_source.all_stats_for_rse(rse)]
            
            um_total = um_success = cc_total = cc_success = 0
            
            um_total = len(um_summaries)
            um_success = len([x for x in um_summaries if x.get("status") == "done"])
            cc_total = len(cc_summaries)
            cc_success = len([x for x in cc_summaries if x.get("status") == "done"])
            
            data[rse] = dict(cc_total=cc_total, um_total=um_total, um_success=um_success, cc_success=cc_success,
                cc_status_history=[
                    {
                        "cc":       x.get("status"),
                        "missing":  x.get("missing_stats",{}).get("action_status"),
                        "dark":     x.get("dark_stats",{}).get("action_status")
                    }
                    for x in cc_summaries
                ][-self.MAX_HISTORY:],
                um_status_history=[x.get("status") for x in um_summaries][-self.MAX_HISTORY:]
            )
        
        return json.dumps(data), "text/json"
        
    def ls(self, request, relpath, rse="*", **args):
        lst = self.App.CCDataSource.ls(rse)
        return ["%s -> %s %s %s %s %s\n" % (d["path"], d["real_path"] or "", d["size"], d["ctime"], d["ctime_text"], d["error"]) for d in lst], "text/plain"
        
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
    # JavaScript Date() takes month starting from 0
    return dt.strftime("new Date(%d, %d, %d)" % (dt.year, dt.month-1, dt.day))  
    
def as_date(t):
    # datetim in UTC
    if t is None:
        return "null"
    dt = datetime.utcfromtimestamp(t)
    # JavaScript Date() takes month starting from 0
    return dt.strftime("%Y/%m/%d")
    
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
    
def none_as_blank(x):
    if x is None:
        return ''
    else:
        return str(x)
        
def format_gigabytes(x):
    x = x * 1024**3 # back to bytes
    mark_letters = " KMGTPX"
    mark_values = [1024**i for i,c in enumerate(mark_letters)]
    the_l, the_v = "", 1
    for l, v in zip(mark_letters, mark_values):
        if x < v:
            break
        the_l, the_v = l, v
    x = x/the_v
    if the_l == " ": the_l = ""
    return "%.1f%s" % (x, the_l)

class App(WPApp):

    Version = Version
    
    def __init__(self, handler, home, cc_path, prefix, wm_path):
        WPApp.__init__(self, handler, prefix=prefix)
        self.CCDataSource = CCDataSource(cc_path)
        self.UMDataSource = UMDataSource(wm_path)
        self.Home = home

    def init(self):
        self.initJinjaEnvironment(tempdirs=[self.Home], 
            filters={
                "hms":hms , "as_dt":as_dt, "as_json":as_json, "path_type":path_type,
                "as_JSON_Date":as_JSON_Date, "none_as_blank":none_as_blank,
                "as_date":as_date, "format_gigabytes":format_gigabytes
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
    home = os.path.dirname(__file__) or "."
    App(Handler, home, cc_path, prefix, wm_path).run_server(port, logging=logging, debug=debug)

        
        
        
        
        
