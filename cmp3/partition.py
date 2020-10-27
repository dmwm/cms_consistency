from part import part, PartitionList
import sys, getopt, re, gzip
from config import Config
try:
    import tqdm
    Use_tqdm = True
except:
    Use_tqdm = False


Usage = """
python partition.py -o <output prefix> <file> ...

Optional:    
            -q - quiet
            -c <config file> 
            -r <rse> - RSE name
            -n <nparts> - override the value from the <config file>
            -z - use gzip compression for output
"""


def main():
    opts, args = getopt.getopt(sys.argv[1:], "n:o:c:qr:z:")
    opts = dict(opts)
    if not args or not ("-o" in opts):
        print(Usage)
        sys.exit(2)


    
    nparts = None
    out_prefix = opts["-o"]
    rewrite_match = rewrite_out = None
    if "-c" in opts:
        rse = opts["-r"]
        config = Config(opts.get("-c"))
        rewrite = config.import_param(rse, "rewrite")
        rewrite_match = re.compile(rewrite["match"]) if rewrite else None
        rewrite_out = re.compile(rewrite["out"]) if rewrite else None
        nparts = config.nparts(rse)
    zout = opts.get("-z", "") in ("out", "both")
    nparts = int(opts.get("-n", nparts))
    
    if nparts is None:
        print("N parts must be specified either with -n or via the -c <config> and -r <rse>")
        print(Usage)
        sys.exit(2)
    
    if zout:
        parts = [gzip.open("%s.%06d.gz" % (out_prefix, i), "wt") for i in range(nparts)]
    else:
        parts = [open("%s.%06d" % (out_prefix, i), "w") for i in range(nparts)]
        
    
    for inp_path in args:
        try:    
                inp = gzip.open(inp_path, "rt") 
                l = inp.readline()
        except:
                inp = open(inp_path, "r")  
                l = inp.readline()
        while l:
            path = l.strip()
            if path:
                if rewrite_match is not None:
                    if not rewrite_match.search(path):
                        sys.stderr.write(f"Path rewrite pattern did not find a match in path {path}\n")
                        sys.exit(1)
                    path = rewrite_match.sub(rewrite_out, path)
                ipart = part(nparts, path)
                parts[ipart].write(path+"\n")
            l = inp.readline()
                
    [out.close() for out in parts]
   
if __name__ == "__main__":
    main()
    
    