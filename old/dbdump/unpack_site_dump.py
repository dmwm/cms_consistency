import sys, getopt
from partition import part
from cPickle import load

Usage = """
python unpack_site_dump.py [-n <nparts>] <input .pkl file> <output prefix>
"""

opts, args = getopt.getopt(sys.argv[1:], "n:")
opts = dict(opts)
if len(args) < 2:
	print (Usage)
	sys.exit(2)

nparts = int(opts.get("-n", 1))
input_file = args[0]
out_prefix = args[1]

if nparts == 1:
	parts = [open(out_prefix, "w")]
else:
	parts = [open("%s.%05d" % (out_prefix, i) for i in range(nparts)]

data = load(open(input_file, "r"))
for f in data.get_files():
	f = f + "\n"
	parts[part(nparts, f)].write(f)

[p.close() for p in parts]

