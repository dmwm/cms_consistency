import random, string, sys, glob

import os

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

def cmp3_parts(parts_dir):
	a_part_names = sorted(glob.glob("%s/a.list.[0-9][0-9][0-9]" % (parts_dir,)))
	r_part_names = sorted(glob.glob("%s/r.list.[0-9][0-9][0-9]" % (parts_dir,)))
	b_part_names = sorted(glob.glob("%s/b.list.[0-9][0-9][0-9]" % (parts_dir,)))

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

def main():

	if len(sys.argv[1:]) < 1:
		print (Usage)
		sys.exit(2)

	parts_dir = sys.argv[1]
	output_dir = sys.argv[2] if sys.argv[2:] else parts_dir

	d, m = cmp3_parts(parts_dir)

	fd = open(f"{output_dir}/d.list","w")
	fm = open(f"{output_dir}/m.list","w")
	for x in d:
		fd.write(x)			# training newlines are there already
	for x in m:
		fm.write(x)
	fd.close()
	fm.close()

		

		



if __name__ == "__main__":
	main()
		

