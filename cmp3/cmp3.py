import random, string, sys, glob, time
from cmplib import cmp3_parts

import os

Usage = """
python cmp3.py [-s <stats file>] <b prefix> <r prefix> <a prefix> <dark output> <missing output>
"""


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

def main():
        import getopt, json

        t0 = time.time()

        opts, args = getopt.getopt(sys.argv[1:], "s:")
        opts = dict(opt)

        if len(args) < 5:
                print (Usage)
                sys.exit(2)

        stats = None
        stats_file = opts.get("-s")
        if stats_file:
            stats = json.loads(open(stats_file, "r").read())

        b_prefix, r_prefix, a_prefix, out_dark, out_missing = args

        d, m = cmp3_parts(b_prefix, r_prefix, a_prefix)

        fd = open(out_dark, "w")
        fm = open(out_missing, "w")
        for x in d:
                fd.write(x)                     # trailing newlines are there already
        for x in m:
                fm.write(x)
        fd.close()
        fm.close()

        print("Found %d dark and %d missing replicas" % (len(d), len(m)))
        t = int(time.time() - t0)
        s = t % 60
        m = t // 60
        print("Elapsed time: %dm%02ds" % (m, s))
        
        if stats is not None:
            stats["cmp3"] = {
                "elapsed": t,
                "missing": len(m),
                "dark": len(d)
            }
            open(stats_file, "w").write(json.dumps(stats))
                

                



if __name__ == "__main__":
        main()
                

