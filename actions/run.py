import glob, re, sys, os, json
from datetime import datetime, timedelta

class CCRun(object):
    def __init__(self, dir_path, rse, run):
        self.Path = dir_path
        self.Run = run
        self.RSE = rse
        self.Timestamp = CCRun.parse_run(run)
        self.Stats = CCRun.get_stats(dir_path, rse, run)
        
    def is_complete(self):
        return self.Stats.get("cmp3", {}).get("status") == "done"
        
    def dark_list_path(self):
        return self.Stats["cmp3"]["dark_list_file"]

    def missing_list_path(self):
        return self.Stats["cmp3"]["missing_list_file"]
        
    def scanner_num_files(self):
        scanner_stats = self.Stats["scanner"]
        nfiles = scanner_stats.get("total_files") or sum(root_stats.get("files", 0) for root_stats in scanner_stats.get("roots", []))
        if nfiles <= 0:
            raise ValueError("Number of files found my the scanner not found or is 0")
        return nfiles

    FileNameRE = re.compile(r"""
            (?P<rse>\w+?)
            (_(?P<timestamp>\d{4}_\d{2}_\d{2}_\d{2}_\d{2}))?
            _(?P<type>[A-Za-z]+)
            \.(?P<ext>.+)
        """, re.VERBOSE)
        
    @staticmethod
    def parse_filename(fn):
        # filename looks like this:
        #
        #   <rse>_%Y_%m_%d_%H_%M_<type>.<extension>
        #   <rse>_<type>.<extension>
        #
        m = CCRun.FileNameRE.match(fn)
        if not m:
            return None, None, None, None
        return m["rse"], m["timestamp"], m["type"], m["ext"]

    @staticmethod
    def parse_run(run):
        # returns datetime object
        yy, mm, dd, h, m = tuple(int(x) for x in run.split("_", 4))
        return datetime(yy, mm, dd, h, m)

    @staticmethod
    def run_ids_for_rse(dir_path, rse):
        files = glob.glob(f"{dir_path}/{rse}_*_stats.json")
        runs = []
        for path in files:
            fn = path.rsplit("/",1)[-1]
            if os.stat(path).st_size > 0:
                r, timestamp, typ, ext = CCRun.parse_filename(fn)
                if r == rse:
                    # if the RSE was X, then rses like X_Y will appear in this list too, 
                    # so double check that we get the right RSE
                    runs.append(timestamp)
        return sorted(runs)
        
    @staticmethod
    def runs_for_rse(dir_path, rse, complete_only=True):
        runs = (CCRun(dir_path, rse, run_id) for run_id in CCRun.run_ids_for_rse(dir_path, rse))
        if complete_only:
            runs = (run for run in runs if run.is_complete())
        return runs
            
    @staticmethod
    def get_stats(dir_path, rse, run):
        path = f"{dir_path}/{rse}_{run}_stats.json"
        stats = json.load(open(path, "r"))
        return stats
    
    def file_list(self, path):
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield line

    def missing_files(self):
        print("missing file list:", self.missing_list_path())
        return self.file_list(self.missing_list_path())

    def dark_files(self):
        return self.file_list(self.dark_list_path())
        
        
        