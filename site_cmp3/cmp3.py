import random, string, sys, glob, time

import os

Usage = """
python cmp3.py <b prefix> <r prefix> <a prefix> <dark output> <missing output>
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
	#print("memory utilization at the end of cmp3, MB:", getMemory())
	return list(d), list(m)

def lines(f):
	l = f.readline()
	while l:
		yield l
		l = f.readline()

def cmp3_parts(a_prefix, r_prefix, b_prefix):
	a_part_names = sorted(glob.glob("%s.*" % (a_prefix,)))
	r_part_names = sorted(glob.glob("%s.*" % (r_prefix,)))
	b_part_names = sorted(glob.glob("%s.*" % (b_prefix,)))

	assert len(a_part_names) == len(r_part_names) and len(a_part_names) == len(b_part_names), "Inconsistent number of parts"

	print ("%d parts found for each list" % (len(a_part_names),))

	d_list, m_list = [], []
	for i, (an, rn, bn) in enumerate(zip(a_part_names, r_part_names, b_part_names)):
		#print("Comparing %s %s %s..." % (an, rn, bn))
		d, m = cmp3(
			lines(open(an, "r")),
			lines(open(rn, "r")),
			lines(open(bn, "r"))
		)
		d_list += d
		m_list += m
		print("Partition %d compared: dark:%d missing:%d" % (i, len(d), len(m))) 
	return d_list, m_list

def main():
	import getopt

	t0 = time.time()

	opts, args = getopt.getopt(sys.argv[1:], "")

	if len(args) < 5:
		print (Usage)
		sys.exit(2)

	b_prefix, r_prefix, a_prefix, out_dark, out_missing = args

	d, m = cmp3_parts(b_prefix, r_prefix, a_prefix)

	fd = open(out_dark, "w")
	fm = open(out_missing, "w")
	for x in d:
		fd.write(x)			# training newlines are there already
	for x in m:
		fm.write(x)
	fd.close()
	fm.close()

	print("Found %d dark and %d missing replicas" % (len(d), len(m)))
	t = int(time.time() - t0)
	s = t % 60
	m = t // 60
	print("Elapsed time: %dm%02ds" % (m, s))
		

		



if __name__ == "__main__":
	main()
		

