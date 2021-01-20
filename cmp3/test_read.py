import sys, time


Usage = """
python test_read.py <input directory> 

	will read files a.list, b.list, r.list in the <input directory> just to measure time
"""


def main():

	if len(sys.argv[1:]) < 1:
		print (Usage)
		sys.exit(2)

	parts_dir = sys.argv[1]

	t0 = time.time()
	
	a = open("%s/a.list" % (parts_dir,), "r")
	b = open("%s/b.list" % (parts_dir,), "r")
	r = open("%s/r.list" % (parts_dir,), "r")
	#
	# simulate simultaneous reading of the 3 files
	#
	a_eof = b_eof = r_eof = False
	a_n = b_n = r_n = 0
	while not (a_eof and b_eof and r_eof):
		if not a_eof: a_eof = not a.readline()
		if not b_eof: b_eof = not b.readline()
		if not r_eof: r_eof = not r.readline()
		if not a_eof:	a_n += 1
		if not b_eof:	b_n += 1
		if not r_eof:	r_n += 1

	t = time.time() - t0
	m = int(t / 60)
	s = t - m*60
	print("Lines read: %d, %d, %d. Elapsed time: %dm %.3fs" % (a_n, r_n, b_n, m, s))
	

		



if __name__ == "__main__":
	main()
		

