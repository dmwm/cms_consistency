import json, os.path, traceback

class JSONFile(object):
    
    def __init__(self, path, data={}):
        self.Path = path
        self.Data = data.copy()
        self.Created = False

    @staticmethod
    def open(path, create=False, data={}):
        f = JSONFile(path)
        if os.path.isfile(path):
            f.load()
        elif create:
            f.Data = data
            f.save()
            f.Created = True
        else:
            f = None
        return f
        
    def load(self):
        self.Data = json.load(open(self.Path, "r"))

    def save(self):
        open(self.Path, "w").write(json.dumps(self.Data, indent=4))
        
    def __getitem__(self, name):
        return self.Data[name]
        
    def __setitem__(self, key, value):
        self.Data[name] = value
        
    def set_at_path(self, path, value, delimiter='.'):
        path = path.split(delimiter)
        #print("split path:", path)
        o = self.Data
        while path:
            k, path = path[0], path[1:]
            if isinstance(o, list):
                try:    k = int(k)
                except: 
                    raise ValueError(f"Can not convert path element '{k}' to integer to access the list element")
            if not path:
                break
            if k:
                o = o[k]
        o[k] = value
        
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

if __name__ == "__main__":
    import sys, getopt
    
    Usage = """
    python json_file.py [-c] <file_path> <command> <args>
        set <path> "<JSON expression>"
        set <path> - < file.json
        set <path> -t "text"
        set <path> -t -   < file.text
    """

    def do_set(jf, args):
        path = args[0]
        opts, args = getopt.getopt(args[1:], "t")
        opts = dict(opts)
        
        data = args[0]
        if data == "-":
            data = sys.stdin.read()
        if "-t" in opts:
            pass
        else:
            data = json.loads(data)
        jf.set_at_path(path, data)
        jf.save()

    opts, args = getopt.getopt(sys.argv[1:], "c")
    if not args:
        print(Usage)
        sys.exit(2)
    opts = dict(opts)
    jf = JSONFile.open(args[0], "-c" in opts)
    command = args[1]
    command_args = args[2:]
    if command == "set":
        do_set(jf, command_args)
    else:
        print(Usage)
        sys.exit(0)



