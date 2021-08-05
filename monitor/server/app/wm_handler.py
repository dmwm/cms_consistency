from webpie import WPApp, WPHandler
import sys, glob, json, time, os, gzip, re, os.path
from datetime import datetime
from data_source import UMDataSource

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
            

        
class WMHandler(WPHandler):
    
    def version(self, request, replapth, **args):
        return json.dumps(Version), "text/json"
    
    def rses(self, request, replapth, **args):
        ds = self.App.UMDataSource
        data = ds.latest_stats()
        return json.dumps(data), "text/json" 

    def ___stats(self, request, replapth, **args):
        ds = self.App.UMDataSource
        data = ds.latest_stats_per_rse()
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
        ds = self.App.UMDataSource
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
        runs = self.App.UMDataSource.all_stats_for_rse(rse)
        # filter out all errors
        runs = [r for r in runs if not r.get("error")]
        return json.dumps(runs), "text/json"
        
    #
    # GUI
    #
        
    def index(self, request, relpath, **args):
        data = self.App.UMDataSource.latest_stats_per_rse()
        rses = sorted(list(data.keys()))
        return self.render_to_response("wm_index.html", rses = rses, data=data)
        
    def rse(self, request, relpath, rse=None, **args):
        if not rse:
            return "RSE must be specified", 400
        data_source = self.App.UMDataSource

        stats_by_run = data_source.all_stats_for_rse(rse)
        if not stats_by_run:
            return f"Data for RSE {rse} not found", 404
        latest_stats = stats_by_run[-1]
        latest_run = latest_stats["run"]
        
            
        # filter out all errors
        stats_by_run = [r for r in stats_by_run if r.get("status") == "done" and not r.get("error") and r.get("start_time") and r.get("end_time")]
        for r in stats_by_run:
            r["elapsed_time"] = (r["end_time"] - r["start_time"])/3600
            r["start_time_miliseconds"] = int(r["start_time"]*1000)
        raw_latest_stats = data_source.read_stats(rse, run, raw=True)
        return self.render_to_response("wm_rse.html", rse=rse, latest_stats=latest_stats, stats_by_run=stats_by_run,
            raw_latest_stats = raw_latest_stats
        )
        
    def stats(self, request, relpath, rse=None, run=None):
        if run:
            stats = self.App.UMDataSource.read_stats(rse, run)
        elif rse:
            stats = self.App.UMDataSource.latest_stats_for_rse(rse)
        else:
            stats = self.App.UMDataSource.latest_stats_per_rse()
        return json.dumps(stats), "text/json"
        
    def ls(self, request, relpath, rse="*", **args):
        lst = self.App.UMDataSource.ls(rse)
        return ["%s -> %s %s %s %s %s\n" % (d["path"], d["real_path"], d["size"], d["ctime"], d["ctime_text"], d["error"]) for d in lst], "text/plain"
        
    def raw_stats(self, request, relpath, rse=None, run=None):
        f = self.App.UMDataSource.open_stats_file(rse, run)
        return self.read_file(f), "text/json"
        
        
        
        
        
