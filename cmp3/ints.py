import os, random

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

lst = [(random.randint(0,1000000), random.randint(0,1000000)) for _ in range(1000000)]

print (getMemory())
