#!/bin/sh

version="1.0"

echo Scan version: $version

if [ "$1" == "" ]; then
	echo 'Usage: wm_scan.sh <config file> <RSE name> <out dir> [<cert file> [<key file>]]'
	exit 2
fi


config_file=$1
RSE=$2
out=$3
cert=$4
key=$5

echo "config_file:               $config_file"
echo "RSE:                       $RSE"
echo "out:                       $out"
echo "cert:                      $cert"
echo "key:                       $key"

python=${PYTHON:-python3}

echo will use python: $python

file_list_prefix=${out}/${RSE}_files.list

now=`date -u +%Y_%m_%d_%H_%M`
stats=${out}/${RSE}_${now}_stats.json
last_stats=${out}/${RSE}_stats.json

# X509 proxy
if [ "$cert" != "" ]; then
        cp $cert /tmp/cert
        cp $key /tmp/key
        chmod go-rwx /tmp/cert /tmp/key
        chmod go-rwx /tmp/key
        voms-proxy-init -voms cms -rfc -valid 192:00 --cert /tmp/cert --key /tmp/key
fi

export PYTHONPATH=`pwd`/cmp3

$python xrootd_scanner.py -o ${file_list_prefix} -n 1 -R 1 -z -c ${config_file} -s ${stats} ${RSE} 
scan_status=$?

if [ -f ${stats} ]; then
    rm -f ${last_stats}; ln -s ${stats} ${last_stats}
fi

if [ "$scan_status" != "0" ]; then
	rm -f ${file_list_prefix}* 
    echo "Site scan failed. Exiting"
    exit 1
fi

