from webpie import WPApp, WPHandler
import sys, glob, json, time, os, gzip, re, os.path, zlib
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
            
    def json_generator(self, iterable):
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
        
    def zip_generator(self, line_iterator, buf_size = 64000):
        compressor = zlib.compressobj()
        buf = []
        lbuf = 0
        for line in line_iterator:
            out = compressor.compress((line + "\n").encode("utf-8"))
            if out:
                buf.append(out)
                lbuf += len(out)
            if lbuf >= buf_size:
                yield b''.join(buf)
                buf = []
                lbuf = 0
        out = compressor.flush()
        if out:
            buf.append(out)
        if buf:
            yield b''.join(buf)

    def text_generator(self, line_iterator, buf_size = 64000):
        buf = []
        lbuf = 0
        for line in line_iterator:
            buf.append(line + "\n")
            lbuf += len(line)+1
            if lbuf >= buf_size:
                yield ''.join(buf)
                buf = []
                lbuf = 0
        if buf:
            yield ''.join(buf)

    def files(self, request, replapth, rse=None, format="raw", include=None, exclude=None, **args):
        
        if include:
            include = include.split(",")

        if exclude:
            exclude = exclude.split(",")

        ds = self.App.UMDataSource
        
        try:
            if format == "raw":
                f, type = ds.open_file_list(rse, binary=True)
                headers = {
                    "Content-Type":type,
                    "Content-Disposition":"attachment"
                }
                return self.read_file(f), headers
            elif format == "zip-stream":
                headers = {
                    "Content-Type":"application/zip",
                    "Content-Disposition":"attachment"
                }
                return self.zip_generator(ds.file_list_as_iterable(rse, include, exclude)), headers
            elif format == "text":
                headers = {
                    "Content-Type":"text/plain",
                    "Content-Disposition":"attachment"
                }
                return self.text_generator(ds.file_list_as_iterable(rse, include, exclude)), headers
            elif format == "json":
                headers = {
                    "Content-Type":"text/json",
                    "Content-Disposition":"attachment"
                }
                return self.json_generator(ds.file_list_as_iterable(rse, include, exclude)), headers
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
        last_run = data_source.latest_run(rse)

        if not last_run:
            return f"Data for RSE {rse} not found", 404
            
        self.redirect(f"./show_run?rse={rse}&run={last_run}")
        
    def show_run(self, request, relpath, rse=None, run=None, **args):
        if not rse or not run:
            return "RSE and run must be specified", 400
        data_source = self.App.UMDataSource
        
        latest_stats_for_rse = data_source.latest_stats_for_rse(rse)

        run_stats = data_source.read_stats(rse, run)
        raw_stats = data_source.read_stats(rse, run, raw=True)

        return self.render_to_response("wm_run.html", rse=rse, run=run, run_stats=run_stats,
            raw_stats = raw_stats, is_latest_run = run == latest_stats_for_rse["run"]
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
        return ["%s -> %s %s %s %s %s\n" % (d["path"], d["real_path"] or "", d["size"], d["ctime"], d["ctime_text"], d["error"]) for d in lst], "text/plain"
        
    def raw_stats(self, request, relpath, rse=None, run=None):
        f = self.App.UMDataSource.open_stats_file(rse, run)
        return self.read_file(f), "text/json"
        
        
        
        
        
