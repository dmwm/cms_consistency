from part import part, PartitionedList
import sys, getopt, re, gzip
from config import Config
try:
    import tqdm
    Use_tqdm = True
except:
    Use_tqdm = False


Usage = """
python partition.py -o <output prefix> (<file>|<url>) ...

Optional:    
            -q - quiet
            -c <config file> 
            -r <rse> - RSE name - to use RSE-specific configuration, ignored if -c is not used
            -n <nparts> - override the value from the <config file>
            -z - use gzip compression for output
            -f - input file compression format (xz, gz, plain. default:plain)
"""


def main():
    opts, args = getopt.getopt(sys.argv[1:], "n:o:c:qr:zf:")
    opts = dict(opts)
    if not args or not ("-o" in opts):
        print(Usage)
        sys.exit(2)


    
    nparts = None
    out_prefix = opts["-o"]
    rewrite_match = rewrite_out = filter_in = remove_prefix = add_prefix = starts_with = None
    if "-c" in opts:
        rse = opts["-r"]
        config = Config(opts.get("-c"))
        preprocess = config.rse_param(rse, "preprocess")
        if preprocess is not None:
            filter_in = preprocess.get("filter")
            if filter_in is not None:
                #print("filtering:", filter_in)
                filter_in = re.compile(filter_in)
            starts_with = preprocess.get("starts_with")
            remove_prefix = preprocess.get("remove_prefix")
            add_prefix = preprocess.get("add_prefix")
            rewrite = preprocess.get("rewrite", {})
            if rewrite:
                    rewrite_match = re.compile(rewrite["match"])
                    rewrite_out = rewrite["out"]
                #print("rewriting:", rewrite["match"], rewrite["out"])
        nparts = config.nparts(rse)
    zout = "-z" in opts
    nparts = int(opts.get("-n", nparts))
    
    in_compression = opts.get("-f", "plain")
    
    if nparts is None:
        print("N parts must be specified either with -n or via the -c <config> and -r <rse>")
        print(Usage)
        sys.exit(2)
    
    in_lst = PartitionedList.open(files=args, compression=in_compression)
    out_lst = PartitionedList.create(nparts, out_prefix, zout)
    
    lines_in = lines_out = 0
    
    for path in in_lst:
        if lines_in and lines_in % 100000 == 0:
            print("lines out/in:", lines_out, lines_in)
        lines_in += 1
        #print("path:", path)
        if starts_with and not path.startswith(starts_with):    continue
        #print("starts with passed")
        if filter_in is not None and not filter_in.search(path): continue
        #print("filter passed")
        
        if remove_prefix is not None:
            if not path.startswith(remove_prefix):
                sys.stderr.write(f"Path {path} does not begin with prefix {remove_prefix}\n")
                sys.exit(1)
            path = path[len(remove_prefix):]
        #print("prefix removed:", path)
        if add_prefix:
            path = add_prefix + path
        if rewrite_match is not None:
            if not rewrite_match.search(path):
                sys.stderr.write(f"Path rewrite pattern did not find a match in path {path}\n")
                sys.exit(1)
            path = rewrite_match.sub(rewrite_out, path)
        #print("path:", type(path), path)
        out_lst.add(path)
        lines_out += 1
    out_lst.close()
    
   
if __name__ == "__main__":
    main()
    
    
