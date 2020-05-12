import random, string, sys, getopt

import os

Usage = """
python gen.py [-r <error rate>] <n> <outpit dir>
	Will generate 3 files under <outpit dir>: a.list, b.list, c.list
	each file will be almost the same, containing almost <n> entries each.
	Each entry will be removed from each file with probability given by -r.
	Default error rate is 0.01
"""
	
	

alphabet = string.ascii_letters + string.digits + "/"

def random_name(l):
	return "/" + "".join(random.choices(alphabet, k=l-1))
	
def gen3(n, r):
	# generates 3 almost identical lists. r controls the "errors"

	for _ in range(n):
		x = random_name(100)
		yield tuple(None if r > random.random() else x for _ in (0,0,0))

def gen(n):
	return [random_name(100) for _ in range(n)]

def gen3(n, r):
	paths = [random_name(100)+'\n' for _ in range(n)]
	return (
		[x for x in paths if random.random() > r],
		[x for x in paths if random.random() > r],		# to generate more dark files
		[x for x in paths if random.random() > r]
	)

opts, args = getopt.getopt(sys.argv[1:], "r:")

if len(args) < 2:
	print (Usage)
	sys.exit(2)


opts = dict(opts)
error_rate = float(opts.get("-r", 0.01))

n = int(sys.argv[1])
out_dir = sys.argv[2]

k = 10000
fa = open(f"{out_dir}/a.list", "w")
fr = open(f"{out_dir}/r.list", "w")
fb = open(f"{out_dir}/b.list", "w")
done = 0

nd = nm = 0

while n > 0:
	nn = min(n, k)
	a, r, b = gen3(nn, error_rate)


	fa.write(''.join(a))
	fr.write(''.join(r))
	fb.write(''.join(b))

	sa = set(a)
	sb = set(b)
	sr = set(r)

	nd += len(sr - sa - sb)
	nm += len((sa&sb) - sr)

	n -= nn
	done += nn
	if done % 100000 == 0:
		print(done)
fa.close()
fb.close()
fr.close()

print("Generated: %d, dark: %d, missing: %d" % (done, nd, nm))
