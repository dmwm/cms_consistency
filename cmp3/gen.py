import random, string, sys

import os

tmpdir = "/data/ivm3/cmp3"

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
		[x for x in paths if random.random() > r],
		[x for x in paths if random.random() > r]
	)

n = int(sys.argv[1])
k = 1000
assert n % k == 0
fa = open(f"{tmpdir}/a.list", "w")
fr = open(f"{tmpdir}/r.list", "w")
fb = open(f"{tmpdir}/b.list", "w")
done = 0
while done < n:
	a, r, b = gen3(k, 0.1)
	fa.write(''.join(a))
	fr.write(''.join(r))
	fb.write(''.join(b))
	done += k
	if done % 100000 == 0:
		print(done)
fa.close()
fb.close()
fr.close()
