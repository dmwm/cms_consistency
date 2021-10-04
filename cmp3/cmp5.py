import random, string, sys, glob, time, gzip
from cmplib import cmp3_generator

from part import PartitionedList


import os
from stats import Stats

Version = "cmp5 1.0"



Usage = """
python cmp5.py [-z] [-s <stats file> [-S <stats key>]] <b m prefix> <b d prefix> <r prefix> <a m prefix> <a d prefix> <dark output> <missing output>
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
        compress = "-z" in opts
        stats_file = opts.get("-s")
        stats_key = opts.get("-S", "cmp3")
        stats = Stats(stats_file) if stats_file else None

        b_m_prefix, b_d_prefix, r_prefix, a_m_prefix, a_d_prefix, out_dark, out_missing = args

        a_m_list = PartitionedList.open(a_m_prefix)
        a_d_list = PartitionedList.open(a_d_prefix)
        r_m_list = PartitionedList.open(r_prefix)
        r_d_list = PartitionedList.open(r_prefix)
        b_m_list = PartitionedList.open(b_m_prefix)
        b_d_list = PartitionedList.open(b_d_prefix)

        my_stats= {
                "version": Version,
                "elapsed": None,
                "start_time": t0,
                "end_time": None,
                "missing": None,
                "dark": None,
                
                "missing_list_file": None,
                "dark_list_file": None,
                
                "b_m_prefix": b_m_prefix,
                "b_d_prefix": b_d_prefix,
                "a_m_prefix": a_m_prefix,
                "a_d_prefix": a_d_prefix,
                "r_prefix": r_prefix,

                "a_m_files": a_m_list.FileNames,
                "b_m_files": b_m_list.FileNames,
                "a_d_files": a_d_list.FileNames,
                "b_d_files": b_d_list.FileNames,

                "a_m_nfiles": a_m_list.NParts,
                "b_m_nfiles": b_m_list.NParts,
                "a_d_nfiles": a_d_list.NParts,
                "b_d_nfiles": b_d_list.NParts,
                
                "r_nfiles": r_d_list.NParts,

                "status": "started"
            }
        
        if stats is not None:
            stats[stats_key] = my_stats

        if compress:
            if not out_dark.endswith(".gz"):    out_dark += ".gz"
            if not out_missing.endswith(".gz"):    out_missing += ".gz"
            fd = gzip.open(out_dark, "wt")
            fm = gzip.open(out_missing, "wt")
        else:
            fd = open(out_dark, "w")
            fm = open(out_missing, "w")

        diffs_m = cmp3_generator(a_m_list, r_m_list, b_m_list, 'm')
        nm = nd = 0
        for path in diffs_m:
            fm.write(path+"\n")
            nm += 1
        fm.close()

        diffs_d = cmp3_generator(a_d_list, r_d_list, b_d_list, 'd')
        for path in diffs_d:
            fd.write(path+"\n")
            nd += 1
        fd.close()

        print("Found %d dark and %d missing replicas" % (nd, nm))
        t1 = time.time()
        
        my_stats.update({
                "elapsed": t1-t0,
                "end_time": t1,
                "missing": nm,
                "dark": nd,
                "status": "done",
                "missing_list_file": out_missing,
                "dark_list_file": out_dark
            })
                
        if stats is not None:
            stats[stats_key] = my_stats

        t = int(t1 - t0)
        s = t % 60
        m = t // 60
        print("Elapsed time: %dm%02ds" % (m, s))
        
if __name__ == "__main__":
        main()
                

