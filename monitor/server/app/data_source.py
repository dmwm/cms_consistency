import os, glob, json, time, os, gzip, os.path, sys

class DataSource(object):
    
    def __init__(self, path):
        self.Path = path
        
    def is_mounted(self):
        return os.path.isdir(self.Path)

    def status(self):
        if not os.path.isdir(self.Path):
            return "Data volume %s does not exist" % (self.Path,)
        return "OK"
        
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
        
    def parse_stats_path(self, path):
        fn = path.split("/")[-1]
        rse, run, typ, ext = self.parse_filename(fn)
        assert typ == "stats" and ext == "json", f"Expected file name to be *_stats.json, path is '{path}', parts: {rse}:{run}:{typ}:{ext}"
        return rse, run

    def list_rses(self):
        files = glob.glob(f"{self.Path}/*_stats.json")
        rses = set()
        for path in files:
            fn = path.rsplit("/",1)[-1]
            rse, timestamp, typ, ext = self.parse_filename(fn)
            rses.add(rse)
        return sorted(list(rses))

    def read_stats(self, rse, run, path=None, raw=False):
        path = path or f"{self.Path}/{rse}_{run}_stats.json"
        try:
            data = json.loads(open(path, "r").read())
            if not raw:
                data.setdefault("run", run)
                data.setdefault("rse", rse)
        except Exception as e:
            if not raw:
                data = {
                    "rse":  rse,
                    "run":  run,
                    "error":    str(e)
                }
            else:
                raise
        return self.postprocess_stats(data) if not raw else data
        
    # overridable - will be called immediatelty after read_stats()
    def postprocess_stats(self, data):
        return data
        
    def latest_stats_per_rse(self):
        out = {}
        for rse in self.list_rses():
            stats = self.latest_stats_for_rse(rse)
            #print(f"latest_stats_per_rse: stats for rse {rse}:", stats)
            if stats is not None:
                out[rse] = stats
        return out
        
    def latest_stats_for_rse(self, rse):
        files = sorted(glob.glob(f"{self.Path}/{rse}_*_stats.json"))
        latest_file = None
        latest_run = None
        for path in files:
            tup = self.parse_stats_path(path)
            if tup:
                r, run = tup
                if r == rse:
                    latest_file = path
                    latest_run = run
        if latest_file:
            return self.read_stats(rse, latest_run, path=latest_file)
        else:
            return None
            
    def all_stats_for_rse(self, rse):
        out = []
        files = sorted(glob.glob(f"{self.Path}/{rse}_*_stats.json"))
        for path in files:
            _, run = self.parse_stats_path(path) 
            data = self.read_stats(rse, run, path=path)
            out.append(data)
        return out

    def ls(self, rse="*", run="*", typ="*"):
        pattern = f"{self.Path}/{rse}_{run}_{typ}.*"
        files = sorted(glob.glob(pattern))
        out = []
        for path in files:
            d = { "path": path, "error":"",
                "size": None, "ctime":None, "ctime_text":None
            }
            try:
                d["real_path"] = os.path.realpath(path)
            except Exception as e:
                d["error"] = str(e)
            try:
                d["size"] = os.path.getsize(path)
                d["ctime"] = os.path.getctime(path)
                d["ctime_text"] = time.ctime(os.path.getctime(path))
            except Exception as e:
                d["error"] = str(e)
            out.append(d)
        #print("ls: out:", out)
        sys.stdout.flush()
        return out
        
    def open_stats_file(self, rse, run):
        path = f"{self.Path}/{rse}_{run}_stats.json"
        return open(path, "r")
        
        
class UMDataSource(DataSource):
    
    def postprocess_stats(self, data):
        out = {"run": data["run"], "rse":data["rse"]}
        if "error" in data:
            out["error"] = data["error"]
        scanner_data = data.get("scanner", {})
        roots = scanner_data.get("roots",[])
        root_data = roots[0] if roots else {}
        out.update(scanner_data)
        out.update(root_data)
        return out

    def run_summary(self, stats):
        summary = {k:stats.get(k) for k in [
            "start_time", "end_time", "status", "error", "files", "run"
        ]}
        #print("UMDataSource.run_summary: stats['files']=", stats.get("files"), "   summary['files']=", summary["files"])
        if summary.get("start_time") and summary.get("end_time"):
            summary["elapsed_time"] = summary["end_time"] - summary["start_time"]
        else:
            summary["elapsed_time"] = None
        return summary
        
    def file_list_as_file(self, rse):
        path = f"{self.Path}/{rse}_files.list"
        if os.path.isfile(path):
            f = open(path, "rb")
            type = "text/plain"
        elif os.path.isfile(path + ".gz"):
            f = open(path + ".gz", "rb")
            type = "application/x-gzip"
        else:
            raise FileNotFoundError("not found")
        return f, type
        
    file_list = file_list_as_file
    
    def line_iterator(self, f):
        while True:
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if line:
                yield line
        
    def file_list_as_iterable(self, rse):
        path = f"{self.Path}/{rse}_files.list"
        if os.path.isfile(path):
            f = open(path, "r")
        elif os.path.isfile(path + ".gz"):
            f = gzip.open(path + ".gz", "rt")
        else:
            raise FileNotFoundError("not found")
        return self.line_iterator(f)
        
    def fill_missing_scanner_parts(self, rse_info):
        rse_stats = {
            k: rse_info.get(k) for k in ["scanner", "server_root", "server", "start_time", "end_time", "status"]
        }
        if "roots" in rse_info:
            for r in rse_info["roots"]:
                if r["root"] == "unmerged":
                    for k in ["error", "root_failed", "failed_subdirectories", "files", "directories", "empty_directories"]:
                        rse_stats[k] = r.get(k)
                    break
        return rse_stats
        
class CCDataSource(DataSource):
    
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
        
    def fill_missing_scanner_parts(self, rse_info):
        rse_stats = {
            k: rse_info.get(k) for k in ["scanner", "server_root", "server", "start_time", "end_time", "status"]
        }
        if "roots" in rse_info:
            for r in rse_info["roots"]:
                if r["root"] == "unmerged":
                    for k in ["error", "root_failed", "failed_subdirectories", "files", "directories", "empty_directories"]:
                        rse_stats[k] = r.get(k)
                    break
        return rse_stats

    NLAST_RUNS = 8
    
    def list_runs(self, rse, nlast=NLAST_RUNS):
        files = glob.glob(f"{self.Path}/{rse}_*_stats.json")
        runs = []
        for path in files:
            fn = path.rsplit("/",1)[-1]
            if os.stat(path).st_size > 0:
                r, timestamp, typ, ext = self.parse_filename(fn)
                if r == rse:
                    # if the RSE was X, then rses like X_Y will appear in this list too, 
                    # so double check that we get the right RSE
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
                    for root_info in scanner_stats.get("roots", []):
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
        ndark = nmissing = confirmed_dark = None
        if "cmp3" in stats:
            ndark = stats["cmp3"].get("dark")
            nmissing = stats["cmp3"].get("missing")
        confirmed_dark = stats.get("cc_dark",{}).get("confirmed_dark_files")
        for k in ["dbdump_before", "scanner", "dbdump_after", "cmp3", "cc_dark", "cc_miss", "cmp2dark"]:
            d = stats.get(k)
            if isinstance(d, dict) and not "elapsed" in d:
                d["elapsed"] = None
                if d.get("end_time") is not None and d.get("start_time") is not None:
                    d["elapsed"] = d["end_time"] - d["start_time"]
        return stats, ndark, nmissing, confirmed_dark
        
    def get_dark_or_missing(self, rse, run, typ, limit):
        path = f"{self.Path}/{rse}_{run}_{typ}.list"
        path_gz = path + ".gz"
        try:
            f = gzip.open(path_gz, "rt")
        except:
            try:    f = open(path, "r")
            except:
                return None
                
        def limited_line_reader(f, n):
            while n is None or n > 0:
                l = f.readline()
                if not l:
                    break
                l = l.strip()
                if l:
                    yield l
                    if n is not None:
                        n -= 1        
            f.close()
            
        return limited_line_reader(f, limit)
            
    def get_dark(self, rse, run, limit=None):
        return self.get_dark_or_missing(rse, run, "D", limit)

    def get_missing(self, rse, run, limit=None):
        return self.get_dark_or_missing(rse, run, "M", limit)
        
    def last_stats(self, rse):
        last_run = self.list_runs(rse, 1)
        if last_run:
            last_run = last_run[0]
            return (last_run,) + self.get_stats(rse, last_run)
        else:
            return None

    def files(self, rse, typ="*"):
        files = sorted(glob.glob(f"{self.Path}/{rse}_{typ}.*"))
        return files
        
    def open_file(self, relpath):
        return open(self.Path+"/"+relpath, "r")

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
                comp_done = comp_status == "done" or comp_status is None and comp_stats.get("end_time") is not None
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
        
        #last_comp = stats.get(self.COMPONENTS[-1])
        #if last_comp:
        #    if last_comp.get("status") == "done":
        #        all_done = True
        
        if all_done:
            status = "done"
        else:
            tend = None
            
        summary = {
            "status": status,
            "run":  stats.get("run"),
            "start_time": tstart,
            "end_time": tend,
            "failed": failed_comp,
            "running": running_comp,            
            "missing_stats" : {
                "detected":         None,
                "acted_on":         None,
                "action_status":    None
            },
            "dark_stats": {
                "detected":         None,
                "confirmed":        None,
                "acted_on":         None,
                "action_status":    None
            }
        }
        
        if "error" in stats:
            summary["error"] = stats["error"]
        
        if "cmp3" in stats and stats["cmp3"]["status"] == "done":
            summary["missing_stats"]["detected"] = stats["cmp3"]["missing"]
            summary["dark_stats"]["detected"] = stats["cmp3"]["dark"]
            
            if "cmp2dark" in stats:
                summary["dark_stats"]["confirmed"] = stats["cmp2dark"].get("join_list_files")

            if "cc_dark" in stats:
                summary["dark_stats"]["acted_on"] = stats["cc_dark"].get("confirmed_dark_files")
                summary["dark_stats"]["action_status"] = stats["cc_dark"].get("status", "").lower() or None
                
            if "cc_miss" in stats:
                summary["missing_stats"]["acted_on"] = stats["cc_miss"].get("confirmed_miss_files")
                if summary["missing_stats"]["acted_on"] is None:
                    summary["missing_stats"]["acted_on"] = stats["cc_miss"].get("confirmed_dark_files")       # there used to be a typo in older versions 
                summary["missing_stats"]["action_status"] = stats["cc_miss"].get("status", "").lower() or None
        
        return summary
