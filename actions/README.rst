Consistency Enforcement Actions
===============================

Missing Action
--------------

.. code-block:: shell

python missing_action.py [options] <storage_path> <rse> <scope> 
    -m <max age, days>             - Max age of the latest run in days, default: 1
    -o (-|<out file>)              - produce dark list and write it to the file or stdout if "-", 
                                     instead of sending to Rucio

Dark Action
-----------

.. code-block:: shell

python dark_action.py [options] <storage_path> <rse>
    -f <ratio, floating point>  - max allowed fraction of confirmed dark files to total number of files found by the scanner,
                                  default = 0.05
    -m <days>                   - max age for the most recent run, default = 1
    -M <days>                   - max age for oldest run to use for confirmation, default = 14
    -n <number>                 - min number of runs to use to produce the confirmed dark list, 
                                  including the most recent run, default = 2
    -o (-|<out file>)           - produce dark list and write it to the file or stdout if "-", instead of sending to Rucio
