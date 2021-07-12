from webpie import WPApp, WPHandler
import sys, glob, json, time, os, gzip, re
from datetime import datetime

Version = "1.1"

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
        
    def stats(self):
        data = self.latest_stats()
        stats = { rse_info["rse"]:self.convert_rse_item(rse_info) for rse_info in data }
        return stats
        
    def stats_for_rse(self, rse):
        files = sorted(glob.glob(f"{self.Path}/{rse}_*_stats.json"))
        latest_file = None
        for path in files:
            tup = self.parse_stats_path(path)
            if tup:
                r, run = tup
                if r == rse:
                    latest_file = path
        if latest_file:
            data = json.loads(open(path, "r").read())["scanner"]
            return self.convert_rse_item(data)
        else:
            return None

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
        data = ds.stats()
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
        
    #
    # GUI
    #
        
    def index(self, request, relpath, **args):
        data = self.App.WMDataSource.stats()
        rses = sorted(list(data.keys()))
        return self.render_to_response("wm_index.html", rses = rses, data=data)
        
    def rse(self, request, relpath, rse=None, **args):
        data = self.App.WMDataSource.stats_for_rse(rse)
        return self.render_to_response("wm_rse.html", rse=rse, data=data)
        
    def ls(self, request, relpath, rse=None, **args):
        lst = self.App.WMDataSource.ls(rse)
        return ["%s -> %s %s %s %s %s\n" % (d["path"], d["real_path"], d["size"], d["ctime"], d["ctime_text"], d["error"]) for d in lst], "text/plain"
        
        
            
        
        
        
        
