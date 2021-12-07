RAL Consistency Checking
========================

RAL_dbdump.sh
-------------

Start this script 1 hour before start of the tape dump procedure at RAL

Usage:

.. code-block:: shell

    $ RAL_dbdump.sh <config.yaml> <dbconfig.cfg> <RSE> <scratch dir> <output dir>
    

RAL_compare.sh
--------------
 
Start this script close to the expected end time of the tape dump at RAL

Usage:

.. code-block:: shell

    $ RAL_compare.sh <config.yaml> <dbconfig.cfg> <RSE> <scratch dir> <output dir> [<cert file> <key file>]
    
The script will make 6 attempts to download the tape dump file with 10 minute interval

    

