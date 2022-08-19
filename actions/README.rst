Consistency Enforcement Actions
===============================

Declaring missing replicas
--------------------------

.. code-block:: shell

    $ python declare_missing.py [options] <storage_path> <scope> <rse>
        -s <stats file>             - file to write stats to
        -S <stats key>              - key to store stats under, default: "missing_action"
        -o (-|<out file>)           - produce confirmed missing list and write it to the file or stdout if "-", instead of sending to Rucio
        -c <config.yaml>|rucio      - load configuration from a YAML file or Rucio
        -v                          - verbose output

        The following will override values read from the configuration:
        -f <ratio>                  - max allowed fraction of confirmed missing files to total number of files found by the scanner,
                                      floating point, default = 0.05
        -m <days>                   - max age for the most recent run, integer, default = 1 day

Declaring dark replicas
-----------------------

.. code-block:: shell

    $ python declare_dark.py [options] <storage_path> <rse>
        -o (-|<out file>)           - write confirmed dark list and write it to the file or stdout if "-", instead of sending to Rucio
        -s <stats file>             - file to write stats to
        -S <stats key>              - key to store stats under, default: "dark_action"
        -c <config.yaml>|rucio      - load configuration from a YAML file or Rucio
        -v                          - verbose output

        The following will override values read from the configuration:
        -f <ratio, floating point>  - max allowed fraction of confirmed dark files to total number of files found by the scanner,
                                      default = 0.05
        -m <days>                   - max age for the most recent run, default = 1 day
        -M <days>                   - max age for oldest run to use for confirmation, default = 15 days
        -n <number>                 - min number of runs to use to produce the confirmed dark list, default = 3

Configuration
-------------

These tools can load their configuration from YAML file or from Rucio.
If YAML file is used, the following fields will be used:

.. code-block:: yaml

    rses:
        "*":            # default for all RSEs
            missing_action:
                max_fraction: <floating pont>               # (missing file count)/(total file count) ratio 
                                                            # threshold to abort the missing action
                max_age_last_run: <int>                     # maximum age of the last run to be used
            dark_action:
                max_fraction: <floating pont>               # (missing file count)/(total file count) ratio 
                                                            # threshold to abort the missing action
                max_age_last_run: <int>                     # maximum age of the last run to be used
                max_age_first_run: <int>                    # maximum age of the first run to be used
                min_runs: <int>                             # minimum number of runs to use from first to last
        RSE:
            # same parameters as for default ("*")
            


