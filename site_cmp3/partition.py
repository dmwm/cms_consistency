from zlib import adler32
from py3 import to_bytes, PY3

def part(nparts, path):
        if PY3:    path = to_bytes(path)
        return adler32(path) % nparts
