from webpie import WPApp, WPHandler
import sys, glob, json, time, os, gzip
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

    def list_rses(self):
        files = glob.glob(f"{self.Path}/*_stats.json")
        rses = []
        for path in files:
            try:
                data = json.loads(open(path, "r").read())
                data = data["scanner"]
                if "rse" in data: rses.append(data)
            except:
                pass
        return sorted(rses, key=lambda d: d["rse"])
        
    def file_list(self, rse):
        path = f"{self.Path}/{rse}_files.list.00000"
        if os.path.isfile(path):
            f = open(path, "rb")
            type = "text/plain"
        elif os.path.isfile(path + ".gz"):
            f = open(path + ".gz", "rb")
            type = "application/x-gzip"
        return f, type
        
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
        data = self.list_rses()
        stats = { rse_info["rse"]:self.convert_rse_item(rse_info) for rse_info in data }
        return stats
        
    def stats_for_rse(self, rse):
        path = f"{self.Path}/{rse}_stats.json"
        data = json.loads(open(path, "r").read())["scanner"]
        return self.convert_rse_item(data)
        
    
        
class WMHandler(WPHandler):
    
    def version(self, request, replapth, **args):
        return json.dumps(Version), "text/json"
    
    def rses(self, request, replapth, **args):
        ds = self.App.WMDataSource
        data = ds.list_rses()
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
    
    def files(self, request, replapth, rse=None, **args):
        ds = self.App.WMDataSource
        f, type = ds.file_list(rse)
        headers = {
            "Content-Type":type,
            "Content-Disposition":"attachment"
        }
        return self.read_file(f), headers
        
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
        
        
            
        
        
        
        
