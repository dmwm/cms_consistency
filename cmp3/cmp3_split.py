import random, string, sys

import os

tmpdir = "/data/ivm3/cmp3"

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

def partition(lst, part_names):
	files = [open(pn, "w") for pn in part_names]
	n = len(part_names)
	for l in lst:
		i = hash(l) % n
		files[i].write(l)
	[f.close() for f in files]

def lines(f):
	l = f.readline()
	while l:
		yield l
		l = f.readline()

def cmp3_partition(a, r, b, prefix, nparts):
	a_part_names = ["%s.a.%d.list" % (prefix, i) for i in range(nparts)]
	r_part_names = ["%s.r.%d.list" % (prefix, i) for i in range(nparts)]
	b_part_names = ["%s.b.%d.list" % (prefix, i) for i in range(nparts)]

	partition(a, a_part_names)
	print("A partitioned")
	partition(r, r_part_names)
	print("R partitioned")
	partition(b, b_part_names)
	print("B partitioned")

	d_list, m_list = [], []
	for i, (an, rn, bn) in enumerate(zip(a_part_names, r_part_names, b_part_names)):
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
		fa = open(f"{tmpdir}/a.list", "r")
		fr = open(f"{tmpdir}/r.list", "r")
		fb = open(f"{tmpdir}/b.list", "r")

		d, m = cmp3_partition(lines(fa), lines(fr), lines(fb), f"{tmpdir}/tmp", 10)
		fd = open(f"{tmpdir}/d.list","w")
		fm = open(f"{tmpdir}/m.list","w")
		for x in d:
			fd.write(x)			# training newlines are there already
		for x in m:
			fm.write(x)
		fd.close()
		fm.close()

		

		



if __name__ == "__main__":
	main()
		

