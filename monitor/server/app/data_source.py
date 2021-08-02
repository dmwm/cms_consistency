import os, glob, json, time, os, gzip, os.path

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

    def latest_stats(self):
        files = sorted(glob.glob(f"{self.Path}/*_stats.json"))
        latest_files = {}
        for path in files:
            tup = self.parse_stats_path(path)
            if tup:
                rse, run = tup
                latest_files[rse] = (path, run)
        
        out = []
        for rse, (path, run) in latest_files.items():
            try:
                data = json.loads(open(path, "r").read())
                if "rse" in data:
                    data["run"] = data.get("run", run)      # fill if missing
                    if not "elapsed_time" in data:
                        if "start_time" in data and "end_time" in data:
                            data["elapsed_time"] = data["end_time"] - data["start_time"]
                        else:
                            data["elapsed_time"] = None
                    out.append(data)
            except:
                pass
        return sorted(out, key=lambda d: d["rse"])
        
    def latest_stats_per_rse(self):
        data = self.latest_stats()
        stats = { rse_info["rse"]:rse_info for rse_info in data }
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
                data = json.loads(open(path, "r").read())
                data["run"] = data.get("run", run)      # fill if missing
            except:
                raise JSONParseError(path)
            return data
        else:
            return None
            
    def all_stats_for_rse(self, rse):
        out = []
        files = sorted(glob.glob(f"{self.Path}/{rse}_*_stats.json"))
        for path in files:
            _, run = self.parse_stats_path(path)
            try:
                data = json.loads(open(path, "r").read())
            except:
                data = {    "error":    f"JSON parse error in {path}"   }
            data["run"] = data.get("run", run)      # fill if missing
            out.append(data)
        out = sorted(out, key=lambda d:d["run"])
        return out

    def ls_stats(self, rse=None):
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
        
