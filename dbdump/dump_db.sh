#!/bin/bash

# 
# Usage: dump_db.sh <config file> <RSE> <output file>
#

rse=$2
out_dir=$3
outfile=

python replicas_for_rse.py -c $1 -o $3 $2
