import sys, glob, time, os

from part import PartitionedList
from stats import Stats


Version = "1.0"



Usage = """
python cmp2.py [-z] [-s <stats file> [-S <stats key>]]    (join|minus|xor|or) <A prefix> <B prefix> <output prefix>
python cmp2.py [-z] [-s <stats file> [-S <stats key>]] -f (join|minus|xor|or) <A file> <B file> <output file>
"""

def main():
    import getopt

    t0 = time.time()

    opts, args = getopt.getopt(sys.argv[1:], "s:S:zf")
    opts = dict(opts)
    


    if len(args) < 4:
            print (Usage)
            sys.exit(2)

    stats_file = opts.get("-s")
    stats_key = opts.get("-S", "join")
    compress = "-z" in opts
    single_file = "-f" in opts

    my_stats = stats = None

    op, a_spec, b_spec, out_spec = args

    if single_file:
        a_list = PartitionedList.open(files=[a_spec])
        b_list = PartitionedList.open(files=[b_spec])
        out_list = PartitionedList.create_file(out_spec)
    else:
        a_list = PartitionedList.open(prefix=a_spec)
        b_list = PartitionedList.open(prefix=b_spec)
        if a_list.NParts != b_list.NParts:
            print("Inconsistent number of parts: %s:%d: %s:%d" % (a_spec, a_list.NParts, b_spec, b_list.NParts))
            sys.exit(1)
        out_list = PartitionedList.create(a_list.NParts, out_spec)
        
    if stats_file is not None:
        stats = Stats(stats_file)
        my_stats= {
            "version": Version,
            "elapsed": None,
            "start_time": t0,
            "end_time": None,
            
            "a_list_files": 0,
            "b_list_files": 0,
            "join_list_files": 0,
            
            "operation":    op,
            
            "b_prefix": b_spec,
            "a_prefix": a_spec,
            "out_prefix": out_spec,

            "a_files": a_list.FileNames,
            "b_files": b_list.FileNames,
            "out_files": out_list.FileNames,

            "nparts": a_list.NParts,

            "status": "started"
        }
        stats[stats_key] = my_stats

    n_a_files = 0
    n_b_files = 0
    n_out_files = 0
    
    for pa, pb in zip(a_list.parts(), b_list.parts()):
        b_set = set(pb)
        n_b_files += len(b_set)
        for f in pa:
            n_a_files += 1
            if op == "and":
                if f in b_set:
                    out_list.add(f)
                    n_out_files += 1
            elif op == "minus":
                if not f in b_set:
                    out_list.add(f)
                    n_out_files += 1
            elif op == "xor":
                if f in b_set:
                    b_set.remove(f)
                else:
                    out_list.add(f)
                    n_out_files += 1
            elif op == "or":
                if f in b_set:
                    b_set.remove(f)
                out_list.add(f)
                n_out_files += 1                
        if op in ("or", "xor"):
            for f in b_set:
                out_list.add(f)
                n_out_files += 1
                
    t1 = time.time()
    
    if stats_file:
        my_stats.update({
            "elapsed": t1-t0,
            "end_time": t1,
            "a_list_files": n_a_files,
            "b_list_files": n_b_files,
            "join_list_files": join_list_files,
            "status": "done"
        })
        stats[stats_key] = my_stats
        
                    
if __name__ == "__main__":
    main()