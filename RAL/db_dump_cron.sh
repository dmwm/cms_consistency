#!/bin/bash

#
# Usage:
#   db_dump_cron.sh <config.yaml> <dbconfig.cfg> <RSE> <output dir>
#

cd ~/RAL

config=$1
dbconfig=$2
rse=$3
outdir=$4

start=`date "+%Y%m%d_%H%M%S"`
tmpdir=${outdir}/${now}.tmp

rm -rf ${outdir}/*.tmp

python db_dump.py -c $config -d $dbconfig -o $tmpdir
exit=$?
if [ "$exit" == "0"]; then
    end=`date "+%Y%m%d_%H%M%S"`
    mv $tmpdir ${outdir}/${end}.db_dump
else
    rm -rf ${outdir}/*.tmp
    exit $exit
fi


