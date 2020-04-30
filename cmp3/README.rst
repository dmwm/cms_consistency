3-way list comparison
=====================

The idea behind this is to compare 3 lists of replicas (DB dump before (B), RSE dump (R) and DB dump after (A)) using Python set functionality instead of pre-soring the files. In terms of sets, the results are:

.. code-block:: 

  M (missing files) = (A-R)*B
  D (dark files) = (R-A)-B

In order to do this in a lineary scalable way, the input files A,B and R need to be split into parts using a suitable hash function. Each part should be up to ~10M entries or less in order for the comparison to fit into several GB RAM.

Scrpits
-------

* gen.py - generates 3 lists of replicas with given rate of "errors". All 3 lists are almost the same, except each file can be randomly removed from each list with given probability. The script produces files a.list, b.list and r.list in given directory.

* split.py - splits a file produced by gen.py into parts according to hash function. For speed, adler32 hashing is used.

* cmp3_parts.py - compares lists of files split into parts found in given directory and produces m.list and d.list files with "missing" and "dark" files respectively.

Timing
------

Splitting one 100M entries file into 10 10M files takes about 4 minutes

Comparing 3 100M entry lists split into 10 parts each takes about 7.5 minutes





