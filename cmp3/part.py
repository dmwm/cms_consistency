from zlib import adler32
import gzip, glob
from py3 import to_bytes, PY3


def part(nparts, path):
        if nparts <= 1: return 0
        if PY3:    path = to_bytes(path)
        #print("part(", nparts, path,"): adler:", adler32(path))
        return adler32(path) % nparts
        
class _Partition(object):
    
    def __init__(self, f):
        self.F = f
        
    def __iter__(self):
        return self
        
    def __next__(self):
        l = self.F.readline()
        if not l:
            raise StopIteration
        return l.strip()
        
class PartitionedList(object):
    
    def __init__(self, mode, filenames, compressed=False, input_compression=None):
        #
        # mode: "r" or "w"
        #
        self.Mode = mode
        self.FileNames = filenames
        self.Files = []
        self.NParts = len(filenames)
        self.Compressed = compressed
        self.InFormat = input_compression
        
        if mode == "w":
            self.Files = [open(fn, "w") if not compressed else gzip.open(fn, "wt") for fn in self.FileNames]
            
    @staticmethod
    def open(prefix=None, files=None, compression=None):
        # open existing set
        if files is None:
            files = sorted(glob.glob(f"{prefix}.*"))
        return PartitionedList("r", files, input_compression=compression)
        
    @staticmethod
    def create(nparts, prefix, compressed=False):
        # create new set
        gz = ".gz" if compressed else ""
        files = ["%s.%05d%s" % (prefix, i, gz) for i in range(nparts)]
        return PartitionedList("w", files, compressed)
        
    def add(self, item):
        if self.Mode != "w":    raise ValueError("The list is not open for writing")
        item = item.strip()
        i = part(self.NParts, item)
        #print(item, "%", self.NParts, "->", i)
        item = item+"\n"
        self.Files[i].write(item)
        
    def files(self):
        return self.Files
        
    def ____items(self):
        assert self.Mode == "r"
        for f in self.Files:
            l = f.readline()
            while l:
                yield l.strip()
                l = f.readline()
                
    def items(self):
        assert self.Mode == "r"
        for fn in self.FileNames:
            in_format = self.InFormat
            if in_format is None:
                if fn.lower().endswith(".gz"):  in_format = "gz"
                elif fn.lower().endswith(".xz"):  in_format = "xz"
                else:   in_format = "plain"
            if fn.startswith("http://") or fn.startswith("https://"):
                import requests
                response = requests.get(fn, stream=True)
                in_file = response.raw
            else:
                in_file = open(fn, "rb")

            if in_format == "gz":
                import gzip
                in_file = gzip.open(in_file, "rb")
            elif in_format == "xz":
                from lzma import LZMAFile
                in_file = LZMAFile(in_file)
                
            l = in_file.readline()
            while l:
                l = l.decode("utf-8").strip()
                #print(l)
                yield l
                l = in_file.readline()
                
    def __iter__(self):
        return self.items()

    def close(self):
        [f.close() for f in self.Files]

    def __del__(self):
        self.close()

if __name__ == "__main__":
    import sys, glob
    prefix = sys.argv[1]
    lst = PartitionedList.open(prefix=prefix)
    for f in lst:
        print (f)
