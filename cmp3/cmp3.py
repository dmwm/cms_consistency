import random, string, sys, glob, time
from cmplib import cmp3_lists

from part import PartitionedList


import os
from stats import Stats

Version = "1.1"



Usage = """
python cmp3.py [-z] [-s <stats file> [-S <stats key>]] <b prefix> <r prefix> <a prefix> <dark output> <missing output>
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

        opts, args = getopt.getopt(sys.argv[1:], "s:S:z")
        opts = dict(opts)

        if len(args) < 5:
                print (Usage)
                sys.exit(2)

        stats_file = opts.get("-s")
        stats_key = opts.get("-S", "cmp3")
        stats = Stats(stats_file) if stats_file else None

        b_prefix, r_prefix, a_prefix, out_dark, out_missing = args

        a_list = PartitionedList.open(a_prefix)
        r_list = PartitionedList.open(r_prefix)
        b_list = PartitionedList.open(b_prefix)

        my_stats= {
                "version": Version,
                "elapsed": None,
                "start_time": t0,
                "end_time": None,
                "missing": None,
                "dark": None,
                "b_prefix": b_prefix,
                "a_prefix": a_prefix,
                "r_prefix": r_prefix,

                "a_files": a_list.FileNames,
                "b_files": b_list.FileNames,
                "r_files": r_list.FileNames,

                "a_nfiles": a_list.NParts,
                "b_nfiles": b_list.NParts,
                "r_nfiles": r_list.NParts,

                "status": "started"
            }
        
        if stats is not None:
            stats[stats_key] = my_stats

        d, m = cmp3_lists(a_list, r_list, b_list)

        fd = open(out_dark, "w")
        fm = open(out_missing, "w")
        for x in d:
                fd.write(x)                     # trailing newlines are there already
        for x in m:
                fm.write(x)
        fd.close()
        fm.close()

        print("Found %d dark and %d missing replicas" % (len(d), len(m)))
        t1 = time.time()
        
        my_stats.update({
                "elapsed": t1-t0,
                "end_time": t1,
                "missing": len(m),
                "dark": len(d),
                "status": "done"
            }}
                
        if stats is not None:
            stats[stats_key] = my_stats

        t = int(t1 - t0)
        s = t % 60
        m = t // 60
        print("Elapsed time: %dm%02ds" % (m, s))
        
if __name__ == "__main__":
        main()
                

