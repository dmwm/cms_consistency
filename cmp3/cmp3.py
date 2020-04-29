import random, string, sys

import os

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

alphabet = string.ascii_letters + string.digits + "/"

def random_name(l):
	return "/" + "".join(random.choices(alphabet, k=l-1))
	

def cmp3(a, r, b):
	#
	# produces 2 lists:
	#
	# 	D = R-A-B
	# 	M = A*B-R
	#
	a_r = set(a)
	r_a = set()
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

def gen3(n, r):
	# generates 3 almost identical lists. r controls the "errors"

	for _ in range(n):
		x = random_name(100)
		yield tuple(None if r > random.random() else x for _ in (0,0,0))

def main():
	if sys.argv[1] == "gen":
		n = int(sys.argv[2])
		fa = open("/tmp/a.list", "w")
		fr = open("/tmp/r.list", "w")
		fb = open("/tmp/b.list", "w")
		for a, r, b in gen3(n, 0.01):
			if a:	fa.write(a + "\n")
			if b:	fb.write(b + "\n")
			if r:	fr.write(r + "\n")
		fa.close()
		fb.close()
		fr.close()

	elif sys.argv[1] == "cmp":
		fa = open("/tmp/a.list", "r")
		fr = open("/tmp/r.list", "r")
		fb = open("/tmp/b.list", "r")

		def lines(f):
			l = f.readline()
			while l:
				yield l
				l = f.readline()

		d, m = cmp3(lines(fa), lines(fr), lines(fb))
		fd = open("/tmp/d.list","w")
		fm = open("/tmp/m.list","w")
		for x in d:
			fd.write(x)			# training newlines are there already
		for x in m:
			fm.write(x)
		fd.close()
		fm.close()

		

		



if __name__ == "__main__":
	main()
		

