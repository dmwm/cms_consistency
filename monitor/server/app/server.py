from webpie import WPApp, WPHandler, WPStaticHandler
import sys, glob, json, time, os, gzip
from datetime import datetime
from um_handler import UMHandler
from ce_handler import CEHandler
from data_source import CCDataSource, UMDataSource, StatsCache

Version = "2.3.5"

def display_file_list(lst):
    Indent = "    "
    last_items = []
    out = []
    for path in lst:
        items = [item for item in path.split("/") if item]
        n_common = 0
        for li, i in zip(items, last_items):
            if li == i:
                n_common += 1
            else:
                break
        tail = items[n_common:]
        indent = Indent * n_common
        for i, item in enumerate(tail):
            if i < len(tail)-1:
                item += "/"
            if not indent: item = "/" + item
            out.append(indent + item)
            indent += Indent
        last_items = items
    return out
    

class Handler(WPHandler):
    
    def __init__(self, *params, **args):
        WPHandler.__init__(self, *params, **args)
        self.unmerged = UMHandler(*params, **args)
        self.ce = CEHandler(*params, **args)

    def new(self, request, relpath, **args):
        # redirect all requests to "new" handler to self
        return self.redirect("../" + (relpath or "index"))

    def index(self, request, relpath, sort="rse", **args):
        return self.redirect("./ce/index")
        
    def probe(self, request, relpath, **args):
        return self.ce.probe(request, relpath, **args)
        
def as_dt(t):
    # datetim in UTC
    if t is None:
        return ""
    dt = datetime.utcfromtimestamp(t)
    return dt.strftime("%Y-%m-%d %H:%M:%S")
    
def as_JSON_Date(t):
    # datetim in UTC
    if t is None:
        return "null"
    dt = datetime.utcfromtimestamp(t)
    # JavaScript Date() takes month starting from 0
    return dt.strftime("new Date(%d, %d, %d)" % (dt.year, dt.month-1, dt.day))  
    
def as_date(t):
    # datetim in UTC
    if t is None:
        return "null"
    dt = datetime.utcfromtimestamp(t)
    # JavaScript Date() takes month starting from 0
    return dt.strftime("%Y/%m/%d")
    
def as_json(d):
    return "\n"+json.dumps(d, indent=4)
    
def hms(t):
    
    if t is None:
        return ""
    if t < 100:
        return "%.2fs" % (t)
    
    t = int(t)
    s = t % 60
    t //= 60
    m = t % 60
    h = t // 60
    
    if h == 0:
        return f"{m}m{s}s"
    else:
        return f"{h}h{m}m"
        
def path_type(path):
    return "dir" if path.endswith("/") else "file"
    
def none_as_blank(x):
    if x is None:
        return ''
    else:
        return str(x)
        
def if_none(x, default=""):
    return default if x is None else x
        
def format_gigabytes(x):
    x = x * 1024**3 # back to bytes
    mark_letters = " KMGTPX"
    mark_values = [1024**i for i,c in enumerate(mark_letters)]
    the_l, the_v = "", 1
    for l, v in zip(mark_letters, mark_values):
        if x < v:
            break
        the_l, the_v = l, v
    x = x/the_v
    if the_l == " ": the_l = ""
    return "%.1f%s" % (x, the_l)

class App(WPApp):

    Version = Version
    
    def __init__(self, handler, home, cc_path, prefix, um_path, um_ignore_list):
        WPApp.__init__(self, handler, prefix=prefix)
        self.CCPath = cc_path
        self.UMPath = um_path
        self.UMIgnoreList = um_ignore_list
        self.Home = home
        self.StatsCache = StatsCache()
        self.StatsCache.init(cc_path)
        self.StatsCache.init(um_path)
        print("Stats cache initialized with", len(self.StatsCache), "entries")

    def init(self):
        self.initJinjaEnvironment(tempdirs=[self.Home], 
            filters={
                "hms":hms , "as_dt":as_dt, "as_json":as_json, "path_type":path_type,
                "as_JSON_Date":as_JSON_Date, "none_as_blank":none_as_blank,
                "as_date":as_date, "format_gigabytes":format_gigabytes,
                "if_none":if_none
            }
        )
        
        
Usage = """
python server.py [-r <url prefix to remove>] <port> <cc data path> <wm data path>
"""

if __name__ == "__main__":
    import sys, getopt

    #print("server.py: sys.argv:", sys.argv)

    opts, args = getopt.getopt(sys.argv[1:], "r:ld", ["um-ignore="])
    opts = dict(opts)

    if not args:
        print (Usage)
        sys.exit(2) 
    
    port = int(args[0])
    cc_path, wm_path = args[1:]
    
    prefix = opts.get("-r")
    logging="-l" in opts
    debug=sys.stdout if "-d" in opts else None

    um_ignore_list = opts.get("--um-ignore", [])
    if um_ignore_list:
        um_ignore_list = um_ignore_list.split(",")

    print("Starting server:\n  port %s\n  CC path %s\n  WM path %s" % (port, cc_path, wm_path))

    sys.stdout.flush()
    home = os.path.dirname(__file__) or "."
    App(Handler, home, cc_path, prefix, wm_path, um_ignore_list).run_server(port, logging=logging, debug=debug)

        
        
        
        
        
