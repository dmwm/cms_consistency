
def cmp3(a, r, b):
    #
    # produces 2 lists:
    #
    #       D = R-A-B = (R-A)-B
    #       M = A*B-R = (A-R)*B
    #
    a_r = set(a)    # this will be A-R
    r_a = set()     # this will be R-A
    for x in r:
            try:    a_r.remove(x)
            except KeyError:
                    r_a.add(x)
    d = r_a
    m = set()
    for x in b:
            try:    d.remove(x)
            except KeyError:    pass
            if x in a_r:
                    m.add(x)
    #print("memory utilization at the end of cmp3, MB:", getMemory())
    return list(d), list(m)

def lines(f):
    l = f.readline()
    while l:
        yield l
        l = f.readline()

def cmp3_parts(a_prefix, r_prefix, b_prefix):
    import glob
    a_part_names = sorted(glob.glob("%s.*" % (a_prefix,)))
    r_part_names = sorted(glob.glob("%s.*" % (r_prefix,)))
    b_part_names = sorted(glob.glob("%s.*" % (b_prefix,)))

    assert len(a_part_names) == len(r_part_names) and len(a_part_names) == len(b_part_names), "Inconsistent number of parts"

    print ("%d parts found for each list" % (len(a_part_names),))

    d_list, m_list = [], []
    for i, (an, rn, bn) in enumerate(zip(a_part_names, r_part_names, b_part_names)):
            #print("Comparing %s %s %s..." % (an, rn, bn))
            d, m = cmp3(
                    lines(open(an, "r")),
                    lines(open(rn, "r")),
                    lines(open(bn, "r"))
            )
            d_list += d
            m_list += m
            print("Partition %d compared: dark:%d missing:%d" % (i, len(d), len(m))) 
    return d_list, m_list
