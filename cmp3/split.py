import random, string, sys, getopt, os, zlib

py3 = sys.version_info >= (3,)

Usage = """
python split.py <input file> <n parts>
"""

if len(sys.argv[1:]) < 2:
	print(Usage)
	sys.exit(2)

input_file = sys.argv[1]
nparts = int(sys.argv[2])

part_names = ["%s.%03d" % (input_file, i) for i in range(nparts)]
parts = [open(fn, "w") for fn in part_names]

in_file = open(input_file, "r")
l = in_file.readline()
while l:
	i = zlib.adler32(bytes(l, "utf-8") if py3 else l) % nparts
	parts[i].write(l)			# this will include training '\n'
	l = in_file.readline()

[part.close() for part in parts]
