import random, string, sys, glob
from zlib import adler32
import os.path
import os

py3 = sys.version_info >= (3,)


Usage = """
python cmp3_parts.py <parts directory> [<output dir>]

	will look for files a.list.*, r.list.*, b.list.* in parts directory
"""


def getMemory():
        # returns memory utilization in MB
        f = open("/proc/%s/status" % (os.getpid(),), "r")
        vmsize = None
        vmrss = None
        for l in f.readlines():
            l = l.strip()
            if l.startswith("VmSize:"):
                vmsize = int(l.split()[1])
            elif l.startswith("VmRSS:"):
                vmrss = int(l.split()[1])
        return float(vmsize)/1024.0, float(vmrss)/1024.0

def cmp3(a, r, b):
	#
	# produces 2 lists:
	#
	# 	D = R-A-B = (R-A)-B
	# 	M = A*B-R = (A-R)*B
	#
	a_r = set(a) 	# this will be A-R
	r_a = set()	# this will be R-A
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
	print("memory utilization at the end of cmp3, MB:", getMemory())
	return list(d), list(m)

def lines(f):
	l = f.readline()
	while l:
		yield l
		l = f.readline()

def cmp3_parts(a_dir, r_dir, b_dir):
	a_part_names = sorted(glob.glob("%s/a.[0-9][0-9][0-9][0-9][0-9]" % (a_dir,)))
	r_part_names = sorted(glob.glob("%s/r.[0-9][0-9][0-9][0-9][0-9]" % (r_dir, )))
	b_part_names = sorted(glob.glob("%s/b.[0-9][0-9][0-9][0-9][0-9]" % (b_dir, )))

	assert len(a_part_names) == len(r_part_names) and len(a_part_names) == len(b_part_names)

	print ("%d parts found for each list" % (len(a_part_names),))

	d_list, m_list = [], []
	for i, (an, rn, bn) in enumerate(zip(a_part_names, r_part_names, b_part_names)):
		print("Comparing %s %s %s..." % (an, rn, bn))
		d, m = cmp3(
			lines(open(an, "r")),
			lines(open(rn, "r")),
			lines(open(bn, "r"))
		)
		d_list += d
		m_list += m
		print(f"Partition {i} compared: dark:{len(d)} missing:{len(m)}") 
	return d_list, m_list
    
def split_file(fn, n_parts, prefix, outdir):
    part_names = ["%s/%s.%05d" % (outdir, prefix, i) for i in range(n_parts)]
    parts = [open(fn, "w") for fn in part_names]
    for l in lines(open(fn, "r")):
        i = adler32(bytes(l, "utf-8") if py3 else l) % n_parts
        parts[i].write(p)
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
        assert os.path.isdir(tempdir), "To compare individual files, a temp directory needs to be provided"
        
        # split files into ~1GB parts
        max_size = max(os.path.getsize(f) for f in (before, storage, after))
        max_part_size = 1000*1000*1000
        n_parts = (max_size + max_part_size - 1) // max_part_size
        
        if n_parts == 1:
            # no need to split
            d, m = cmp3(
                lines(open(after, "r")),
                lines(open(storage, "r")),
                lines(open(before, "r"))
            )
        else:
            if tempdir is None:
                tempdir = os.path.dirname(out)      # assume its ok to write temp files into the output directory
            tmp_names = split_file(after, n_parts, "a", tempdir) \
                + split_file(before, n_parts, "b", tempdir) \
                + split_file(storage, n_parts, "r", tempdir)
            d, m = cmp3_parts(n_parts, tempdir, tempdir, tempdir)            
            for fn in tmp_names: os.remove(fn)  # remove temo files
    else:
        # files are pre-split and named a.#####, b.#####, r.###### in respective directories
        assert os.path.isdir(storage) and os.path.isdir(after)
        n = len(glob.glob("%s/a.*" % (after,)))
        assert n == len(glob.glob("%s/b.*" % (before,))) and n == len(glob.glob("%s/r.*" % (storage,)))
        d, m = cmp3_parts(before, storage, after)
    
    with open(out, "w") as out_f:
        for p in m: out_f.write("LOST,%p\n" % (p,))
        for p in d: out_f.write("DARK,%p\n" % (p,))

Usage = """
Usage: python consistency.py [-t <tmp dir>] <before> <storage> <after> <output file>
<before>, <after> and <storage> can be either files or directories.
If directories, they must contain files like b.001, b.002, ... a.001, a.002, ..., r.001, r.002, ... respectively
If <tmp dir> is not given, then the <output file>'s directory will be used
"""
        
def main():
    import getopt
    
    opts, args = getopt.getopt(sys.argv[1:], "t:")
    opts = dict(opt)
    
    temp_dir = opts.get("-t")
    if len(args) != 4:
        print(Usage)
        sys.exit(2)
        
    before, storage, after, out = args
    
    consistency(before, storage, after, out, temp_dir)


if __name__ == "__main__":
	main()
		

