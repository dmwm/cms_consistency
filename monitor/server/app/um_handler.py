from webpie import WPHandler, WPStaticHandler
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

class UMHandler(WPHandler):
    
    def __init__(self, *params, **args):
        WPHandler.__init__(self, *params, **args)
        self.DataSource = UMDataSource(self.App.UMPath, self.App.StatsCache, self.App.UMIgnoreList)
        self.static = WPStaticHandler(*params, **args)
    
    def version(self, request, replapth, **args):
        return json.dumps(Version), "text/json"
    
    def rses(self, request, replapth, **args):
        ds = self.DataSource
        data = ds.latest_stats()
        return json.dumps(data), "text/json" 

    def ___stats(self, request, replapth, **args):
        ds = self.DataSource
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
            item = '%s "%s"' % (',\n' if not first else '', x)
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

        ds = self.DataSource
        
        try:
            if format == "raw":
                f, encoding = ds.open_file_list(rse, binary=True)
                headers = {
                    "Content-Type":"text/plain",
                    "Content-Disposition":"attachment"
                }
                if encoding == "gzip":
                    headers["Content-Type"] = "application/x-gzip"
                    headers["Content-Encoding"] = "gzip"
                return self.read_file(f), headers
            elif format == "zip-stream":
                headers = {
                    "Content-Type":"application/zip",
                    "Content-Encoding":"deflate",
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
        runs = self.DataSource.all_stats_for_rse(rse)
        # filter out all errors
        runs = [r for r in runs if not r.get("error")]
        return json.dumps(runs), "text/json"

    #
    # GUI
    #
        
    def index(self, request, relpath, sort="rse", attention=False, **args):
        #
        # list available RSEs
        #
        um_data_source = self.DataSource
        attention = attention == "yes"

        um_stats = um_data_source.latest_stats_per_rse()
        summaries = [(rse, um_data_source.run_summary(stats)) for rse, stats in um_stats.items()]
        for rse, summary in summaries:
            summary["rse"] = rse
        summaries = [summary for _, summary in summaries]

        if attention:
            summaries = [s for s in summaries if s["status"] != "done"]
            sort_order = {
                "failed": 0,
                "started": 1,
                "*": 2
            }
            summaries = sorted(summaries, key=lambda s: (sort_order.get(s["status"], sort_order["*"]), s["rse"]))
        else:
            if sort == "um_run":
                summaries = sorted(summaries, key=lambda s: (s.get("start_time") or -1, x["rse"]))
            elif sort == "-um_run":
                summaries = sorted(summaries, key=lambda s: (s.get("start_time") or -1, x["rse"]), reverse=True)
            else:
                summaries = sorted(summaries, key=lambda s: s["rse"])
        
        return self.render_to_response("um_index.html", infos=summaries, sort_options=not attention)
        
    def show_rse(self, request, relpath, rse=None, **args):
        data_source = self.DataSource
        runs = data_source.list_runs(rse)
        runs = sorted(runs, reverse=True)
        
        summaries = []
        for run in runs:
            stats = data_source.get_stats(rse, run)
            r = data_source.run_summary(stats)
            r["elapsed_time_hours"] = r["start_time_milliseconds"] = None
            if r.get("start_time"):
                r["start_time_milliseconds"] = int(r["start_time"]*1000)
                if r.get("end_time"):
                    r["elapsed_time_hours"] = (r["end_time"] - r["start_time"])/3600
            r.setdefault("total_size_gb", None)
            summaries.append((run, r))
        return self.render_to_response("um_rse.html", rse=rse, summaries=summaries)

    def show_run(self, request, relpath, rse=None, run=None, **args):
        if not rse or not run:
            return "RSE and run must be specified", 400
        data_source = self.DataSource
        
        latest_stats_for_rse = data_source.latest_stats_for_rse(rse)

        run_stats = data_source.read_stats(rse, run)
        raw_stats = data_source.read_stats(rse, run, raw=True)

        return self.render_to_response("um_run.html", rse=rse, run=run, run_stats=run_stats,
            raw_stats = raw_stats, is_latest_run = run == latest_stats_for_rse["run"]
        )
        
    def stats(self, request, relpath, rse=None, run=None):
        if run:
            stats = self.DataSource.read_stats(rse, run)
        elif rse:
            stats = self.DataSource.latest_stats_for_rse(rse)
        else:
            stats = self.DataSource.latest_stats_per_rse()
        return json.dumps(stats), "text/json"
        
    MAX_HISTORY = 10

    def status_history(self, request, relpath, rses=None, **args):
        um_data_source = self.DataSource

        if rses is None:
            rses = set(um_data_source.list_rses())
        else:
            rses = rses.split(",")
            
        data = {}      # {rse -> (cc_total, um_total, cc_success, um_success)}
        
        for rse in rses:
            if not rse: continue
            um_summaries = [um_data_source.run_summary(x) for x in um_data_source.all_stats_for_rse(rse)]
            
            um_total = um_success = 0
            
            um_total = len(um_summaries)
            um_success = len([x for x in um_summaries if x.get("status") == "done"])
            
            data[rse] = dict(um_total=um_total, um_success=um_success, 
                um_status_history=[x.get("status") for x in um_summaries][-self.MAX_HISTORY:]
            )
            
        return json.dumps(data), "text/json"
        
    def ls(self, request, relpath, rse="*", **args):
        lst = self.DataSource.ls(rse)
        return ["%s -> %s %s %s %s %s\n" % (d["path"], d["real_path"] or "", d["size"], d["ctime"], d["ctime_text"], d["error"]) for d in lst], "text/plain"
        
    def raw_stats(self, request, relpath, rse=None, run=None):
        f = self.DataSource.open_stats_file(rse, run)
        return self.read_file(f), "text/json"
        
        
        
        
        
