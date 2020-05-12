from zlib import adler32

def part(nparts, path):
	return adler32(path) % nparts
