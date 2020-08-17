from gzip import GzipFile

Usage = """
pyhton path_list.py compress <input> <putput>
pyhton path_list.py decompress <input> <putput>
"""

from py3 import to_bytes, to_str

class PathListRead(object):
    
    def __init__(self, f):
        self.F = f
        self.LastPath = ""
        
    def read(self):
        done = False
        while not done:
            line = self.F.readline()
            if not line:    return None     # EOF
            line = line.strip()
            if line:
                words = line.split(":",1)
                n = int(words[0])
                tail = words[1]
                self.LastPath = self.LastPath[:n] + tail
                return self.LastPath
    
    def paths(self):
        done = False
        while not done:
            path = self.read()
            if not path:    break
            yield path
            
class PathListWrite(object):

    BUFFER_SIZE = 100000
    
    def __init__(self, f):
        self.F = f
        self.LastPath = []
        self.Buffer = []
        
    def write(self, path):
        print("write: path:", repr(path))
        self.Buffer.append(path)
        if len(self.Buffer) >= self.BUFFER_SIZE:
            self.flush()
    
    def flush(self):
        paths = sorted(self.Buffer)
        for path in paths:
            min_len = min(len(path), len(self.LastPath))
            n = 0
            for n, (a, b) in enumerate(zip(path, self.LastPath)):
                if a != b:  break
            self.F.write("%d:%s" % (n, path[n:]))
            self.LastPath = path
        self.Buffer = []
    
    def close(self):
        self.flush()
        self.F.close()
        
class PathListWrite_gzip(object):

    def __init__(self, f):
        self.F = f
        self.G = GzipFile(fileobj=f, mode="wb")
        
    def write(self, path):
        self.G.write(to_bytes(path+"\n"))
    
    def flush(self):
        pass
    
    def close(self):
        self.G.close()
        
class PathListRead_gzip(object):
    
    def __init__(self, f):
        self.F = f
        self.G = GzipFile(fileobj=f, mode="rb")
        
    def read(self):
        done = False
        while not done:
            line = self.G.readline()
            if not line:    return None     # EOF
            path = line.strip()
            return path
    
    def paths(self):
        done = False
        while not done:
            path = self.read()
            if not path:    break
            yield path
            

        
if __name__ == "__main__":
    import sys
    
    if len(sys.argv[1:]) < 3:
        print (Usage)
        sys.exit(2)

    cmd, inp, out = sys.argv[1:]
    
    if sys.argv[1] == "compress":
        if inp == "-":
            inp = sys.stdin
        else:
            inp = open(inp, "r")
        out = open(out, "wb")
        w = PathListWrite_gzip(out)
        for line in inp.readlines():
            path = line.strip()
            if path:
                w.write(path)
        w.close()
    else:
        inp = open(inp, "rb")
        if out == "-":
            out = sys.stdout
        else:
            out = open(out, "w")
        r = PathListRead_gzip(inp)
        for path in r.paths():
            out.write(to_str(path)+"\n")
        out.close()
    
        
            
            
