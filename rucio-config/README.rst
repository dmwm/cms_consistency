Exporting/Importing CE Configuration
====================================

These tools let you convert Consistency Enforcement configuration between YAML file and the configuration stored in Rucio


Importing configuration from YAML file to Rucio:

.. code-block:: shell

    $ python import_config.py [-c] <config.yaml>
    -c allows to create RSEs, which exist in the YAML file but not in Rucio
    
Print configuration from Rucio to stdout in YAML format:

.. code-block:: shell

    $ python export_config.py

These tools use Rucio client initialized using standard Rucio environment