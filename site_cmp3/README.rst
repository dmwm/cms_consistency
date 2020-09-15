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

Use config.yaml.sample as a template for the configuration file config.yaml.
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
  * workers - integer - number of parallel "xrdfs ls" clients to run
  * remove_prefix - string - additional path prefix to remove from the found path after the server_root was removed
  * add_prefix - string - prefix to add to the path after applying "remove_prefix"
  * rewrite - dictionary - defines further path-to-LFN rewriting rules using RegExp
  
      * path - string, regex pattern - pattern to match the path to
      * out - string, sed-style rewrite rule, using matching sunstrings found by the "path" regexp

  * filter - string, regex pattern - only paths where the pattern is found will be sent to the output
  * roots - list - list of subdirectories to scan
      
      * path - subdirectory relative to the server_root, should not begin with "/"
      * recursion - integer - optional, overrides the site setting
      * timeout - integer - optional, overrides the site setting
      * workers - integer - optional, overrides the site setting
      
Path to LFN conversion
----------------------
For each path:

1. Site root is removed from the head of the path. The resulting path will begin with "/".
2. If "remove prefix" is present for the site, it is removed from the head of the path.
3. If "add prefix" is present, it will be prepended to the path
4. If there is the "rewrite" section for the site, it will be used to search-and-replace portions of the path according to the RegExp specified.

The filter is applied after the Path is converted to LFN


