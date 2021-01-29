from webpie import WPApp, WPHandler
import sys, glob, json, time, os, gzip
from datetime import datetime

Version = "1.0"

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
        
    def stats_for_rse(self, rse):
        path = f"{self.Path}/{rse}_stats.json"
        data = json.loads(open(path, "r").read())
        return data
        
    def file_list(self, rse):
        path = f"{self.Path}/{rse}_files.list.00000"
        if os.path.isfile(path):
            f = open(path, "rb")
            type = "text/plain"
        elif os.path.isfile(path + ".gz"):
            f = open(path + ".gz", "rb")
            type = "application/x-gzip"
        return f, type
        
class WMHandler(WPHandler):
    
    def rses(self, request, replapth, **args):
        ds = self.App.WMDataSource
        data = ds.list_rses()
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
        return self.read_file(f), type
