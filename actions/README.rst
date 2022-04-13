Consistency Enforcement Actions
===============================


.. code-block:: shell

    python cc_action.py (dark|missing) [options] <storage_path> <rse>
        -f <ratio, floating point>  - max allowed fraction of confirmed dark files to total number of files found by the scanner,
                                      default = 0.05
        -m <days>                   - max age for the most recent run, default = 1 day
        -o (-|<out file>)           - produce dark list and write it to the file or stdout if "-", instead of sending to Rucio
        -s <stats file>             - file to write stats to
        -S <stats key>              - key to store stats under, default: "cc_dark"

    dark mode only options:
        -M <days>                   - max age for oldest run to use for confirmation, default = 14 days
        -n <number>                 - min number of runs to use to produce the confirmed dark list, 
                                      including the most recent run, default = 2
