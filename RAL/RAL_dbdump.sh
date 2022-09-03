#!/bin/bash

#
# Usage:
#   RAL_dbdump.sh <config.yaml> <dbconfig.cfg> <RSE> <scratch dir> <output dir>
#

config=$1
rucio_config_file=$2
RSE=$3
scratch=$4
out=$5

if [ ! -f /consistency/config.yaml ]; then
    cp $config /consistency/config.yaml    # to make it editable
    echo Config file $config copied to /consistency/config.yaml
fi
config=/consistency/config.yaml

python=${PYTHON:-python3}

export PYTHONPATH=`pwd`/cmp3:`pwd`

today=`date -u +%Y_%m_%d_00_00`

# hack
#today=2021_08_27_00_00

b_prefix=${scratch}/${RSE}_${today}_B
bm_prefix=${b_prefix}_M
bd_prefix=${b_prefix}_D

stats=${out}/${RSE}_${today}_stats.json
    
echo
echo DB dump before ...
echo

rucio_cfg=""
if [ "$rucio_config_file" != "-" ]; then
    rucio_cfg="-d $rucio_config_file"
fi

rm -rf ${b_prefix}*
#$python cmp3/db_dump.py -o ${b_prefix} -c ${config} $rucio_cfg -s ${stats} -S "dbdump_before" ${RSE} 
$python cmp3/db_dump.py -z -f A:${bm_prefix} -f "*:${bd_prefix}" -c ${config} $rucio_cfg -s ${stats} -S "dbdump_before" ${RSE} 

exit=$?
if [ "$exit" != "0" ]; then
    rm -rf ${b_prefix}*
    exit $exit
fi


