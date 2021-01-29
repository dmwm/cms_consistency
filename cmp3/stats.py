
import json, os.path

class Stats(object):
    
    def __init__(self, path):
        self.Path = path
        self.Data = {}
        
    def __getitem__(self, name):
        return self.Data[name]
        
    def __setitem__(self, name, value):
        self.Data[name] = value
        
    def get(self, name, default=None):
        return self.Data.get(name, default)
        
    def save(self):
        open(self.Path, "w").write(json.dumps(self.Data, indent=4))
        

def write_stats(my_stats, stats_file, stats_key):
    if stats_file:
        if os.path.isfile(stats_file):    
            with open(stats_file, "r") as f:
                stats = json.loads(f.read())
        else:
            stats = {}
        stats[stats_key] = my_stats
        open(stats_file, "w").write(json.dumps(stats))
