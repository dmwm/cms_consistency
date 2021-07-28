from webpie import WPApp, WPHandler
import sys, glob, json, time, os, gzip, re, os.path
from datetime import datetime

Version = "1.1"

class JSONParseError(Exception):
    
    def __init__(self, path):
        self.Path = path
        self.Exists = self.IsFile = self.Size = self.MTime = None
        try:
            self.Exists = os.path.exists(path)
            self.IsFile = os.path.isfile(path)
            self.Size = os.path.getsize()
            self.MTime = os.path.getmtime()
        except:
            pass
            
    def __str__(self):
        return f"Error parsing JSON file {self.Path}"
            

class WMDataSource(object):
    
    def __init__(self, path):
        self.Path = path
        
    def is_mounted(self):
        return os.path.isdir(self.Path)

    def status(self):
        if not os.path.isdir(self.Path):
            return "Data volume %s does not exist" % (self.Path,)
        return "OK"
        
    Run_stats_pattern = re.compile(r"/(?P<dir>.*)/(?P<rse>.*)_(?P<run>\d{4}_\d{2}_\d{2}_\d{2}_\d{2})_stats\.json")

    def parse_stats_path(self, fn):
        m = self.Run_stats_pattern.match(fn)
        if not m:   return None
        return m["rse"], m["run"]

    def latest_stats(self):
        files = sorted(glob.glob(f"{self.Path}/*_stats.json"))
        latest_files = {}
        for path in files:
            tup = self.parse_stats_path(path)
            if tup:
                rse, run = tup
                latest_files[rse] = path
        
        out = []
        for rse, path in latest_files.items():
            try:
                data = json.loads(open(path, "r").read())
                data = data["scanner"]
                if "rse" in data: out.append(data)
            except:
                pass
        return sorted(out, key=lambda d: d["rse"])
        
    def file_list_as_file(self, rse):
        path = f"{self.Path}/{rse}_files.list.00000"
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
        path = f"{self.Path}/{rse}_files.list.00000"
        if os.path.isfile(path):
            f = open(path, "r")
        elif os.path.isfile(path + ".gz"):
            f = gzip.open(path + ".gz", "rt")
        else:
            raise FileNotFoundError("not found")
        return self.line_iterator(f)
        
    def convert_rse_item(self, rse_info):
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
        
    def latest_stat_per_rse(self):
        data = self.latest_stats()
        stats = { rse_info["rse"]:self.convert_rse_item(rse_info) for rse_info in data }
        return stats
        
    def latest_stats_for_rse(self, rse):
        files = sorted(glob.glob(f"{self.Path}/{rse}_*_stats.json"))
        latest_file = None
        for path in files:
            tup = self.parse_stats_path(path)
            if tup:
                r, run = tup
                if r == rse:
                    latest_file = path
        if latest_file:
            try:
                data = json.loads(open(path, "r").read())["scanner"]
            except:
                raise JSONParseError(path)
            return self.convert_rse_item(data)
        else:
            return None
            
    def all_stats_for_rse(self, rse):
        out = []
        files = sorted(glob.glob(f"{self.Path}/{rse}_*_stats.json"))
        for path in files:
            _, run = self.parse_stats_path(path)
            try:
                data = json.loads(open(path, "r").read())["scanner"]
            except KeyError:
                data = {    "error":    f"scanner section not found in {path}"   }
            except:
                data = {    "error":    f"JSON parse error in {path}"   }
            else:
                data = self.convert_rse_item(data)
            data["run"] = run
            out.append(data)
        out = sorted(out, key=lambda d:d["run"])
        return out

    def ls(self, rse=None):
        pattern = f"{self.Path}/*_stats.json" if rse is None else f"{self.Path}/{rse}_*_stats.json"
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
        return out
        
        
        
class WMHandler(WPHandler):
    
    def version(self, request, replapth, **args):
        return json.dumps(Version), "text/json"
    
    def rses(self, request, replapth, **args):
        ds = self.App.WMDataSource
        data = ds.latest_stats()
        return json.dumps(data), "text/json" 

    def stats(self, request, replapth, **args):
        ds = self.App.WMDataSource
        data = ds.latest_stat_per_rse()
        return json.dumps(data), "text/json" 
    
    def read_file(self, f):
        while True:
            buf = f.read(1024*128)
            if not buf:
                break
            yield buf
            
    def json_iterator(self, iterable):
        buf = ["[\n"]
        l = 2
        first = True
        for x in iterable:
            item = '%s "%s"' % (',' if not first else '', x)
            first = False
            buf.append(item)
            l += len(item)
            if l > 100*1000:
                yield ''.join(buf)
                buf = []
                l = 0
        if buf:
            yield ''.join(buf)
        yield "\n]\n"

    def files(self, request, replapth, rse=None, format="raw", **args):
        ds = self.App.WMDataSource
        try:
            if format == "raw":
                f, type = ds.file_list_as_file(rse)
                headers = {
                    "Content-Type":type,
                    "Content-Disposition":"attachment"
                }
                return self.read_file(f), headers
            elif format == "json":
                headers = {
                    "Content-Type":"text/json",
                    "Content-Disposition":"attachment"
                }
                return self.json_iterator(ds.file_list_as_iterable(rse)), headers
        except FileNotFoundError:
            return 404, "not found"
            
    def rse_statistics_data(self, request, relpath, rse=None, **args):
        runs = self.App.WMDataSource.all_stats_for_rse(rse)
        # filter out all errors
        runs = [r for r in runs if not r["error"]]
        return json.dumps(runs), "text/json"
        
    #
    # GUI
    #
        
    def index(self, request, relpath, **args):
        data = self.App.WMDataSource.latest_stat_per_rse()
        rses = sorted(list(data.keys()))
        return self.render_to_response("wm_index.html", rses = rses, data=data)
        
    def rse(self, request, relpath, rse=None, **args):
        if not rse:
            return "RSE must be specified", 400

        stats_by_run = self.App.WMDataSource.all_stats_for_rse(rse)
        if not stats_by_run:
            return f"Data for RSE {rse} not found", 404
        latest_run = stats_by_run[-1]
            
        # filter out all errors
        stats_by_run = [r for r in stats_by_run if r.get("status") == "done" and not r["error"] and r.get("start_time") and r.get("end_time")]
        for r in stats_by_run:
            r["elapsed_time"] = (r["end_time"] - r["start_time"])/3600
            r["start_time_miliseconds"] = int(r["start_time"]*1000)
        return self.render_to_response("wm_rse.html", rse=rse, latest_run=latest_run, stats_by_run=stats_by_run)
        
    
        
    def ls(self, request, relpath, rse=None, **args):
        lst = self.App.WMDataSource.ls(rse)
        return ["%s -> %s %s %s %s %s\n" % (d["path"], d["real_path"], d["size"], d["ctime"], d["ctime_text"], d["error"]) for d in lst], "text/plain"
        
        
            
        
        
        
        
