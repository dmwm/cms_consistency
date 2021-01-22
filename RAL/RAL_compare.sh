#!/bin/bash


#
# Usage:
#   RAL_compare.sh <config.yaml> <dbconfig.cfg> <RSE> <scratch dir> <output dir> [<cert file> [<key file>]]
#

cd ~/RAL

config=$1
rucio_config_file=$2
RSE=$3
scratch=$4
out=$5
cert=$6
key=$7

sleep_interval=1000      # 10 minutes
attempts="1 2 3 4 5 6"


today=`date +%Y_%m_%d_00_00`

b_prefix=${scratch}/${RSE}_${today}_B.list
a_prefix=${scratch}/${RSE}_${today}_A.list
r_prefix=${scratch}/${RSE}_${today}_R.list
stats=${out}/${RSE}_${today}_stats.json

d_out=${out}/${RSE}_${today}_D.list
m_out=${out}/${RSE}_${today}_M.list

tape_dump_tmp=${scratch}/${RSE}_${today}_tape_dump.gz

# X509 proxy
if [ "$cert" != "" ]; then
        if [ "$key" == "" ]; then
            export X509_USER_PROXY=$cert
        else
            voms-proxy-init -voms cms -rfc -valid 192:00 --cert $cert --key $key
        fi
fi

echo
echo Downloading tape dump ...
echo

downloaded="no"

for attempt in $attempts; do
    echo Attempt $attempt ...
    rm -f ${tape_dump_tmp}
    xrdcp root://ceph-gw1.gridpp.rl.ac.uk/cms:/store/accounting/tape/dump_16012022.gz ${tape_dump_tmp}
    if [ "$?" != "0" ]; then
        echo sleeping ...
        sleep $sleep_interval
    else
        echo succeeded
        python partition.py -c $config -r $RSE -q -o ${r_prefix} ${tape_dump_tmp}
        rm -f ${tape_dump_tmp}
        downloaded="yes"
        break
    fi
done

if [ "$downloaded" == "no" ]; then
    exit 1
fi

rucio_cfg=""
if [ "$rucio_config_file" != "-" ]; then
    rucio_cfg="-d $rucio_config_file"
fi

echo
echo DB dump after ...
echo

$python db_dump.py -o ${a_prefix} -c ${config_file} $rucio_cfg -s ${stats} -S "dbdump_after" ${RSE} 
                
echo
echo Comparing ...
echo

python cmp3.py -s ${stats} ${b_prefix} ${r_prefix} ${a_prefix} ${d_out} ${m_out}

echo Dark list:    `wc -l ${d_out}`
echo Missing list: `wc -l ${m_out}`

    

