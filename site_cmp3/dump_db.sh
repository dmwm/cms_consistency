#!/bin/bash

Usage="dump_db.sh <config file> <rucio.cfg> <RSE> <nparts> <output file prefix>"

if [ "$1" == "" ]; then
	echo $Usage
	exit 2
fi


config=$1
dbconfig=$2
rse=$3
nparts=$4
out_prefix=$5

python db_dump.py -d $dbconfig -c $config -o $out_prefix -n $nparts $rse
