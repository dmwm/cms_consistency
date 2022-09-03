from part import part, PartitionedList
import sys, getopt, re, gzip
from config import CEConfiguration
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
            -r <rse> - RSE name - to use RSE-specific configuration, ignored if -c is not used
            -n <nparts> - override the value from the <config file>
            -z - use gzip compression for output
"""


def main():
    opts, args = getopt.getopt(sys.argv[1:], "n:o:c:qr:z")
    opts = dict(opts)
    if not args or not ("-o" in opts):
        print(Usage)
        sys.exit(2)
    
    nparts = None
    out_prefix = opts["-o"]
    rewrite_match = rewrite_out = filter_in = remove_prefix = add_prefix = starts_with = None
    ignore_list = []
    if "-c" in opts:
        rse = opts["-r"]
        cfg = opts["-c"]
        if cfg == "rucio":
            config = CEConfiguration.rse_config(rse, "rucio")
        else:
            config = CEConfiguration.rse_config(rse, "yaml", opts["-c"])
        ignore_list = config.IgnoreList
        nparts = config.NPartitions
    zout = "-z" in opts
    nparts = int(opts.get("-n", nparts))
    
    if nparts is None:
        print("N parts must be specified either with -n or via the -c <config> and -r <rse>")
        print(Usage)
        sys.exit(2)
    
    in_lst = PartitionedList.open(files=args)
    out_lst = PartitionedList.create(nparts, out_prefix, zout)

    #print("ignore list:", ignore_list)
    
    for path in in_lst:
        if starts_with and not path.startswith(starts_with):    continue
        for ignore_path in ignore_list:
            #print(f"checking path {path} for ignore path {ignore_path}")
            if path.startswith(ignore_path):
                ignore=True
                break
        else:
            ignore=False
        if ignore: continue
        if filter_in is not None and not filter_in.search(path): continue
        if remove_prefix is not None:
            if not path.startswith(remove_prefix):
                sys.stderr.write(f"Path {path} does not begin with prefix {remove_prefix}\n")
                sys.exit(1)
            path = path[len(remove_prefix):]
        if add_prefix:
            path = add_prefix + path
        if rewrite_match is not None:
            if not rewrite_match.search(path):
                sys.stderr.write(f"Path rewrite pattern did not find a match in path {path}\n")
                sys.exit(1)
            path = rewrite_match.sub(rewrite_out, path)
        #print("path:", type(path), path)
        out_lst.add(path)
    out_lst.close()
    
    print(out_lst.NWritten)
    
   
if __name__ == "__main__":
    main()
    
    
