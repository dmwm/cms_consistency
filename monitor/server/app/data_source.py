import os, glob, json, time, os, gzip, os.path, sys, re
from pythreader import Primitive, synchronized, RWLock

class StatsCache(Primitive):

    def __init__(self):
        Primitive.__init__(self)
        self.RWLock = RWLock()
        self.Cache = {}             # {path -> (mtime, data)}
        self.HitRatio = 0.0
    
    def init(self, dir_path):
        # pre-read all JSON files
        with self.RWLock.exclusive:
            for path in glob.glob(dir_path + "/*stats.json"):
                try:    self.get(path)
                except: pass

    @synchronized
    def get(self, path):
        mtime = os.path.getmtime(path)
        with self.RWLock.shared:
            tup = self.Cache.get(path)
            if tup:
                my_mtime, data = tup
                if mtime == my_mtime:
                    self.HitRatio = self.HitRatio*0.99 + 0.01
                    return data
        with self.RWLock.exclusive:
            data = json.load(open(path, "r"))
            self.Cache[path] = (mtime, data)
            self.HitRatio = self.HitRatio*0.99
        return data
        
    def __len__(self):
        return len(self.Cache)

class DataSource(object):
    
    def __init__(self, path, cache):
        self.Path = path
        self.Cache = cache
        
    def is_mounted(self):
        return os.path.isdir(self.Path)

    def status(self):
        if not os.path.isdir(self.Path):
            return "Data volume %s does not exist" % (self.Path,)
        return "OK"
        
    def parse_path(self, path):
        dir_path, fn = path.rsplit("/", 1)
        return (dir_path,) + self.parse_filename(fn)
        
    FileNameRE = re.compile(r"""
            (?P<rse>\w+?)
            (_(?P<timestamp>\d{4}_\d{2}_\d{2}_\d{2}_\d{2}))?
            _(?P<type>[A-Za-z]+)
            \.(?P<ext>.+)
        """, re.VERBOSE)
        
    def parse_filename(self, fn):
        # filename looks like this:
        #
        #   <rse>_%Y_%m_%d_%H_%M_<type>.<extension>
        #   <rse>_<type>.<extension>
        #
        m = self.FileNameRE.match(fn)
        if not m:
            return None, None, None, None
        return m["rse"], m["timestamp"], m["type"], m["ext"]
        
    def parse_stats_path(self, path):
        fn = path.split("/")[-1]
        rse, run, typ, ext = self.parse_filename(fn)
        assert typ == "stats" and ext == "json", f"Expected file name to be *_stats.json, path is '{path}', parts: {rse}:{run}:{typ}:{ext}"
        return rse, run

    def list_rses(self):
        files = glob.glob(f"{self.Path}/*_stats.json")
        rses = set()
        for path in files:
            dirpath, rse, timestamp, typ, ext = self.parse_path(path)
            rses.add(rse)
        return sorted(list(rses))

    NLAST_RUNS = 10
    
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
        
    def latest_run(self, rse):
        runs = self.list_runs(rse, 1)
        if not runs:    return None
        else: return runs[-1]
        
    def read_stats(self, rse, run, path=None, raw=False):
        path = path or f"{self.Path}/{rse}_{run}_stats.json"
        try:
#            data = json.loads(open(path, "r").read())
            data = self.Cache.get(path)
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
            
    def all_stats_for_rse(self, rse, limit=NLAST_RUNS):
        out = []
        files = sorted(glob.glob(f"{self.Path}/{rse}_*_stats.json"))
        for path in files:
            r, run = self.parse_stats_path(path) 
            if r == rse:
                data = self.read_stats(rse, run, path=path)
                if data:
                    out.append(data)
        if limit is not None:
            out = out[-limit:]
        return out

    def ls(self, rse="*", run="*", typ="*"):
        pattern = f"{self.Path}/{rse}_{run}_{typ}.*"
        files = set(glob.glob(pattern))
        if run == "*":
            files |= set(glob.glob(f"{self.Path}/{rse}_{typ}.*"))
            if typ == "*":
                files |= set(glob.glob(f"{self.Path}/{rse}_*"))
        files = sorted(list(files))
        out = []
        for path in files:
            if rse != "*":
                _, r, _, _, _ = self.parse_path(path)
                if r != rse:
                    continue
            d = { "path": path, "error":"",
                "size": None, "ctime":None, "ctime_text":None,
                "real_path": None
            }
            try:
                real_path = os.path.realpath(path)
                if real_path != path:
                    d["real_path"] = real_path
                
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
        #sys.stdout.flush()
        return out
        
    def open_stats_file(self, rse, run):
        path = f"{self.Path}/{rse}_{run}_stats.json"
        return open(path, "r")
        
    def files(self, rse, typ="*"):
        files = sorted(glob.glob(f"{self.Path}/{rse}_*_{typ}.*"))
        return files
        
    def open_file(self, path):
        return open(self.Path+"/"+path, "r")
        
class UMDataSource(DataSource):

    def __init__(self, path, cache, ignore_list):
        DataSource.__init__(self, path, cache)
        self.DefaultIgnoreRE = None if not ignore_list else re.compile("^(%s)" % ("|".join(ignore_list),))

    def postprocess_stats(self, data):
        out = {"run": data["run"], "rse":data["rse"]}
        if "error" in data:
            out["error"] = data["error"]
        scanner_data = data.get("scanner", {})
        roots = scanner_data.get("roots",[])
        root_data = roots[0] if roots else {}
        out.update(scanner_data)
        out.update(root_data)
        if out.get("status") != "done":
            out["files"] = None
        out.setdefault("total_size_gb", None)
        return out

    def run_summary(self, stats):
        summary = {k:stats.get(k) for k in [
            "start_time", "end_time", "status", "error", "files", "run", "total_size_gb"
        ]}
        #print("UMDataSource.run_summary: stats['files']=", stats.get("files"), "   summary['files']=", summary["files"])
        if summary.get("start_time") and summary.get("end_time"):
            summary["elapsed_time"] = summary["end_time"] - summary["start_time"]
        else:
            summary["elapsed_time"] = None
        return summary
        
    def open_file_list(self, rse, binary=True):
        files = glob.glob(f"{self.Path}/{rse}_files.list*")
        if files:
            path = files[0]
        else:
            raise FileNotFoundError("not found")
        
        encoding = None
        if binary:
            if path.endswith(".gz"):
                f = open(path, "rb")
                encoding = "gzip"
            else:
                f = open(path, "rb")
        else:
            if path.endswith(".gz"):
                f = gzip.open(path, "rt")
            else:
                f = open(path, "r")
        return f, encoding

    def file_list_as_iterable(self, rse, include=None, exclude=None):
        include_re = exclude_re = None
        if include is None and exclude is None:
            exclude_re = self.DefaultIgnoreRE
        else:
            if include is not None: include_re = re.compile("^(%s)" % ('|'.join(include),))
            if exclude is not None: exclude_re = re.compile("^(%s)" % ('|'.join(exclude),))
        
        f, _ = self.open_file_list(rse, binary=False)
        while True:
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            if include_re is not None and not include_re.match(line):
                continue
            if exclude_re is not None and exclude_re.match(line):
                continue
            yield line

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
        
    def open_file(self, relpath):
        return open(self.Path+"/"+relpath, "r")

        
class CCDataSource(DataSource):
    
    def __init__(self, path, cache, new=False):
        DataSource.__init__(self, path, cache)
        if new:
            self.DarkSection = "dark_action"
            self.MissingSection = "missing_action"
        else:
            self.DarkSection = "cc_dark"
            self.MissingSection = "cc_miss"
    
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
        if not os.path.isfile(path):
            return None
        if typ == "stats":
            #stats = json.loads(f.read())
            stats = self.Cache.get(path)
            if "scanner" in stats:
                scanner_stats = stats["scanner"]
                if not "total_files" in scanner_stats:
                    nfiles = ndirectories = 0
                    for root_info in scanner_stats.get("roots", []):
                        if not root_info.get("root_failed"):
                            nfiles += root_info.get("files", 0)
                            ndirectories += root_info.get("directories", 0)
                    scanner_stats["total_files"] = nfiles
                    scanner_stats["total_directories"] = ndirectories
            out = stats
        else:
            out = []
            with open(path, "r") as f:
                while limit is None or len(out) < limit:
                    l = f.readline()
                    if not l:
                        break
                    l = l.strip()
                    if l:
                        out.append(l)
            return out
        return out
                
    def get_stats(self, rse, run):
        stats = self.get_data(rse, run, "stats")
        ndark = nmissing = confirmed_dark = None
        if "cmp3" in stats:
            ndark = stats["cmp3"].get("dark")
            nmissing = stats["cmp3"].get("missing")
        confirmed_dark = stats.get(self.DarkSection,{}).get("confirmed_dark_files")
        for k in ["dbdump_before", "scanner", "dbdump_after", "cmp3", self.DarkSection, self.MissingSection, "cmp2dark"]:
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
        
    def file_lists_diffs(self, rse, run):
        # compare dark or missing list from the run to the previous run
        # returns (prev_run, missing_old, missing_new, dark_old, dark_new) - sets
        # or (None, None, None, None, None)

        runs = self.list_runs(rse)
        try:    this_i = runs.index(run)
        except ValueError:
            return (None, None, None, None, None)         # run not found

        this_dark = self.get_dark(rse, run)
        this_missing = self.get_missing(rse, run)
        if this_dark is None or this_missing is None:
            return (None, None, None, None, None)         # one of the lists missing

        prev_stats = prev_missing = prev_missing = None
        prev_i = this_i - 1
        if prev_i >= 0:
            prev_run = runs[prev_i]
            prev_stats, _, _, _ = self.get_stats(rse, prev_run)
            if prev_stats is not None and prev_stats.get("cmp3", {}).get("status") == "done":
                prev_dark = self.get_dark(rse, prev_run)
                prev_missing = self.get_missing(rse, prev_run)

        if prev_dark is None or prev_missing is None:
            return (None, None, None, None, None)         # no run to compare to

        this_dark = set(this_dark)
        this_missing = set(this_missing)
        prev_dark = set(prev_dark)
        prev_missing = set(prev_missing)

        return (prev_run, 
            this_missing & prev_missing, this_missing - prev_missing, 
            this_dark & prev_dark, this_dark - prev_dark
        ) 

    def get_dark(self, rse, run, limit=None):
        return self.get_dark_or_missing(rse, run, "D", limit)

    def get_missing(self, rse, run, limit=None):
        return self.get_dark_or_missing(rse, run, "M", limit)
        
    def ___last_stats(self, rse):
        last_run = self.list_runs(rse, 1)
        if last_run:
            last_run = last_run[0]
            return (last_run,) + self.get_stats(rse, last_run)
        else:
            return None, None

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
        
        last_comp = stats.get(self.COMPONENTS[-1])
        if last_comp:
            if last_comp.get("status") == "done" and failed_comp is None:
                all_done = True
        
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

            if self.DarkSection in stats:
                summary["dark_stats"]["acted_on"] = stats[self.DarkSection].get("confirmed_dark_files")
                summary["dark_stats"]["action_status"] = stats[self.DarkSection].get("status", "").lower() or None
                summary["dark_stats"]["aborted_reason"] = stats[self.DarkSection].get("aborted_reason", "")
                
            if self.MissingSection in stats:
                summary["missing_stats"]["acted_on"] = stats[self.MissingSection].get("confirmed_miss_files", 
                                stats[self.MissingSection].get("confirmed_missing_files"))
                summary["missing_stats"]["action_status"] = stats[self.MissingSection].get("status", "").lower() or None
                summary["missing_stats"]["aborted_reason"] = stats[self.MissingSection].get("aborted_reason", "")
        return summary
