import json, os.path, traceback

class Stats(object):
    
    def __init__(self, path):
        self.Path = path
        self.Data = {}
        
    def __getitem__(self, name):
        return self.Data[name]
        
    def __setitem__(self, name, value):
        self.Data[name] = value
        self.save()
        
    def get(self, name, default=None):
        return self.Data.get(name, default)
        
    def update(self, data):
        self.Data.update(data)
        self.save()
        
    def save(self):
        try:
            with open(self.Path, "r") as f:
                data = f.read()
        except:
            traceback.print_exc()
            data = ""
        print("data:", data)
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
python [-k <key>] [-u <update JSON file>] <stats JSON file>
"""

if __name__ == "__main__":
    import sys, getopt
    
    opts, args = getopt.getopt(sys.argv[1:], "k:u:")
    opts = dict(opts)
    
    if not args:
        print(Usage)
        sys.exit(2)
    stats_file = args[0]
    key = opts.get("-k")
    if "-u" in opts:
        update = json.loads(open(opts["-u"], "r").read())
    else:
        update = json.loads(sys.stdin.read())

    s = Stats(stats_file)
    if key:
        s[key] = update
    else:
        s.update(update)
    
    
