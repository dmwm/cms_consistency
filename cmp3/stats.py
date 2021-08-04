import json, os.path, traceback

class Stats(object):
    
    def __init__(self, path):
        self.Path = path
        self.Data = {}
        
    def __getitem__(self, name):
        return self.Data[name]
        
    def overwrite(self, key, value):
        self.Data[name] = value
        self.save()
        
    __setitem__ = overwrite
    
    def get(self, name, default=None):
        return self.Data.get(name, default)
        
    def setdefault(self, name, value):
        if name in self.Data:
            d = self.Data[name]
        else:
            self.Data[name] = value
            d = value
        return d
        
    def update(self, data):
        self.Data.update(data)
        self.save()
        
    def save(self):
        try:
            with open(self.Path, "r") as f:
                data = f.read()
        except:
            #traceback.print_exc()
            data = ""
        #print("data:", data)
        data = json.loads(data or "{}")
        data.update(self.Data)
        open(self.Path, "w").write(json.dumps(data, indent=4))
        

def write_stats(my_stats, stats_file, stats_key = None):
    if stats_file:
        stats = {}
        if os.path.isfile(stats_file):    
            with open(stats_file, "r") as f:
                stats = json.loads(f.read())
        if stats_key:
            stats[stats_key] = my_stats
        else:
            stats.update(my_stats)
        open(stats_file, "w").write(json.dumps(stats))

Usage = """
python [-k <key>] [-u <update JSON file>] [-j "<inline JSON expression>"] [-t] <stats JSON file to update>
"""

if __name__ == "__main__":
    import sys, getopt
    
    opts, args = getopt.getopt(sys.argv[1:], "k:u:j:t")
    opts = dict(opts)
    
    if not args:
        print(Usage)
        sys.exit(2)
    stats_file = args[0]
    key = opts.get("-k")
    if "-u" in opts:
        update = json.loads(open(opts["-u"], "r").read())
    elif "-j" in opts:
        update = json.loads(opts["-j"])
    elif "-t" in opts:
        update = sys.stdin.read()           # treat the input as text value
    else:
        update = json.loads(sys.stdin.read())

    s = Stats(stats_file)
    if key:
        path = key.split("/")
        path, last = path[:-1], path[-1]
        d = s
        for p in path:
            d = d.setdefault(p, {})            
            print(p, d, s.Data)
        d[last] = update
        s.save()
    else:
        s.update(update)
    
    
