import random, string, sys, glob
from zlib import adler32
import os.path
import os

py3 = sys.version_info >= (3,)


PART_SIZE = 1024*1024*1024	# GB
#PART_SIZE = 1024*1024		# MB
Verbose = False

def cmp3(a, r, b):
        #
        # produces 2 lists:
        #
        #       D = R-A-B = (R-A)-B
        #       M = A*B-R = (A-R)*B
        #
        a_r = set(a)    # this will be A-R
        r_a = set()     # this will be R-A
        for x in r:
                if x in a_r:
                        a_r.remove(x)
                else:
                        r_a.add(x)
        d = r_a
        m = set()
        for x in b:
                if x in d:
                        d.remove(x)
                if x in a_r:
                        m.add(x)
        return list(d), list(m)

def cmp3_parts(n, a_dir, r_dir, b_dir):
        a_part_names = sorted(glob.glob("%s/a.list.*" % (a_dir,)))[:n]
        r_part_names = sorted(glob.glob("%s/r.list.*" % (r_dir, )))[:n]
        b_part_names = sorted(glob.glob("%s/b.list.*" % (b_dir, )))[:n]

        assert len(a_part_names) == len(r_part_names) and len(a_part_names) == len(b_part_names)

        if Verbose: print ("%d parts found for each list" % (len(a_part_names),))

        d_list, m_list = [], []
        for i, (an, rn, bn) in enumerate(zip(a_part_names, r_part_names, b_part_names)):
                if Verbose: print("Comparing %s %s %s..." % (an, rn, bn))
                d, m = cmp3(
                        open(an, "r"),
                        open(rn, "r"),
                        open(bn, "r")
                )
                d_list += d
                m_list += m
                if Verbose: print(f"Partition {i} compared: dark:{len(d)} missing:{len(m)}") 
        return d_list, m_list
    
def split_file(fn, n_parts, prefix, outdir):
    part_names = ["%s/%s.%05d" % (outdir, prefix, i) for i in range(n_parts)]
    parts = [open(fn, "w") for fn in part_names]
    for l in open(fn, "r"):
        i = adler32(bytes(l, "utf-8") if py3 else l) % n_parts
        parts[i].write(l)
    [p.close() for p in parts]
    return part_names
    
    
def consistency(before, storage, after, out, tempdir=None):
    #
    # before, storage and after can be either file paths or directory paths
    #
    
    assert os.path.exists(before) and os.path.exists(after) and os.path.exists(storage)
    
    d, m = [], []      
    
    if os.path.isfile(before):
        assert os.path.isfile(storage) and os.path.isfile(after)
        
        # split files into parts
        max_size = max(os.path.getsize(f) for f in (before, storage, after))
        n_parts = (max_size + PART_SIZE - 1) // PART_SIZE
        
        if n_parts == 1:
            # no need to split
            d, m = cmp3(
                open(after, "r"),
                open(storage, "r"),
                open(before, "r")
            )
        else:
            if tempdir is None:
                tempdir = os.path.dirname(out)      # assume its ok to write temp files into the output directory
            assert os.path.isdir(tempdir), "To compare individual files, a temp directory needs to be provided"
            tmp_names = split_file(after, n_parts, "a.list", tempdir) \
                + split_file(before, n_parts, "b.list", tempdir) \
                + split_file(storage, n_parts, "r.list", tempdir)
            d, m = cmp3_parts(n_parts, tempdir, tempdir, tempdir)            
            for fn in tmp_names: os.remove(fn)  # remove temp files
    else:
        # files are pre-split and named a.list.#####, b.list.#####, r.list.###### in respective directories
        assert os.path.isdir(storage) and os.path.isdir(after)
        n = len(glob.glob("%s/a.list.*" % (after,)))
        assert n == len(glob.glob("%s/b.list.*" % (before,))) and n == len(glob.glob("%s/r.list.*" % (storage,)))
        d, m = cmp3_parts(n, before, storage, after)
    
    with open(out, "w") as out_f:
        for p in sorted(m): out_f.write("LOST,%s" % (p,))
        for p in sorted(d): out_f.write("DARK,%s" % (p,))

    return d, m

Usage = """
Usage: 
	
        python consistency.py [-p <part size>] [-t <tmp dir>] <before> <storage> <after> <output file>

<before>, <after> and <storage> can be either files or directories.
If directories, they must contain part files like b.list.001, b.list.002, ... a.list.001, a.list.002, ..., r.list.001, r.list.002, ... respectively
If <tmp dir> is not given, then the <output file>'s directory will be used to partition the input files.

Part size can be specified either as an integer or as <int>k, <int>m, <int>g for kilobytes, megabytes, gigabytes.
Default part size is 1 GB.

If the input is already split into parts and all the parts are in the same directory, you can use this:

	python consistency.py <directory> <output file>

in this case, all the parts for all 3 lists are expected to be in the specified directory and named a.list.*, b.list.*, r.list*

"""
        
if __name__ == "__main__":
    import getopt
    
    opts, args = getopt.getopt(sys.argv[1:], "t:p:")
    opts = dict(opts)
    
    temp_dir = opts.get("-t")
    part_size = opts.get("-p", "1g")

    if part_size[-1] in "kmg":
        PART_SIZE = int(part_size[:-1]) * {
		'g':	1024 * 1024 * 1024,
		'm':	1024 * 1024,
		'k':	1024 
	}[part_size[-1]]
    else:
        PART_SIZE = int(part_size)
    
    if len(args) == 4:
            before, storage, after, out = args
    elif len(args) == 2:
            before = storage = after = args[0] 
            out = args[1]
    else:
        print(Usage)
        sys.exit(2)


        
    
    d, m = consistency(before, storage, after, out, temp_dir)
    nd = len(d)
    nm = len(m)

    print("Found: dark: %d, missing: %d" % (nd, nm))


                

