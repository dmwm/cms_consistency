Installation
============

1. Install cx_Oracle:

	pip install cx-Oracle

2. If necessary, install Oracle Instant Client: https://www.oracle.com/database/technologies/instant-client/downloads.html
	add the directory with Oracle Instant Client to LD_LIBRARY_PATH
    If missing, install libaio using
    
    yum install libaio

3. Install SQL Alchemy:

	pip install SQLAlchemy

4. Install Rucio client environment - this step is no longer needed to run the script

Configuring site_cmp3
=====================

Base configuration
------------------

Use config.yaml.sample as a template for the base configuration file config.yaml.
The config.yaml may or may not contain "database" section. If rucio.cfg is used, then the config.yaml does not need the database section.

The db_dump.py produces list of LFNs for all active replicas found in the Rucio DB for the site. The list is partitioned into smaller
pieces by hashing the LFN. The same partitioning must be used by the xrootd_dump.py. Both scripts use the same hashing algoritm (Adler32).

In the config.yaml, each site is identified by its Rucio RSE name. The top level "rses" dictionary of the configuration is indexed
by the RSE name.

For each RSE, the following parameters may be defined:

* server - string - FQDN for the xrootd server
* server_root - string - the top path of the area to scan in the xrood namespace
* recursion - integer - the level relative to the server_root at which to start using "xrdfs ls -R"
* timeout - integer - timeout in seconds for "xrdfs ls" command
* nworkers - integer - number of parallel "xrdfs ls" clients to run
* remove_prefix - string - additional path prefix to remove from the found path after the server_root was removed
* add_prefix - string - prefix to add to the path after applying "remove_prefix"
* rewrite - dictionary - defines further path-to-LFN rewriting rules using RegExp

    * path - string, regex pattern - pattern to match the path to
    * out - string, sed-style rewrite rule, using matching sunstrings found by the "path" regexp

* filter - string, regex pattern - only paths where the pattern is found will be sent to the output
* roots - list - list of subdirectories to scan
    
    * path - subdirectory relative to the server_root, should not begin with "/"
      
Path to LFN conversion
----------------------
For each path:

1. Site root is removed from the head of the path. The resulting path will begin with "/".
2. If "remove prefix" is present for the site, it is removed from the head of the path.
3. If "add prefix" is present, it will be prepended to the path
4. If there is the "rewrite" section for the site, it will be used to search-and-replace portions of the path according to the RegExp specified.

The filter is applied after the Path is converted to LFN

Parameters controlled by site admin
-----------------------------------
Several configuration parameters can be controlled by the RSE admin by modifying the RSE metadata in Rucio using

.. code-block:: shell

  $ rucio-admin rse set-attribute    --rse <RSE> --key <parameter name> --value <parameter value>
  $ rucio-admin rse delete-attribute --rse <RSE> --key <parameter name> --value <parameter value>
  
The following site attributes are recognized:

Things an RSE admin may want to change:

* CE_config.ce_disabled - can be used to disable CE runs for the RSE. To disable, use "true" as the value, to enable either delete the parameter or set it to "false"
* CE_config.sever - hostname:port for the RSE xrootd server, e.g. "ingrid-se08.cism.ucl.ac.be:1094"
* CE_config.server_root - the top path of the CMS area to scan in the xrood namespace
* CE_config.roots - comma-separated list of top level directories in LFN space to scan, e.g.: "/store/mc,/store/data"
* CE_config.ignore_list - comma-separated list of LFN path prefixes to ignore, e.g. "/store/mc/X,/store/mc/YY"

Things an RSE admin should change only if they know what they are doing:

* CE_config.timeout - integer number of seconds to use as an individual "ls" command timeout
* CE_config.nworkers - integer number of parallel "xrdfs ls" clients to run
* CE_config.max_dark_fraction - floating point maximum fraction of confirmed dark files to allow the dark action. Default: 0.01
* CE_config.max_missing_fraction - floating point maximum fraction of missing files to allow the missing action. Default: 0.01

When the same value is present in the base configuraion file and in the RSE attributes, the value from the RSE attributes
will be used, which allows the RSE admin to override the configuration values for the RSE.

If you want to keep the changes for forseeable future, it is good idea to notify those who maintain the base configuration file
to incorporate your changes into the base configuraton so they are recorded permanently.