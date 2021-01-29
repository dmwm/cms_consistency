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

python=${PYTHON:-python}

echo will use python: $python

file_list_prefix=${out}/${RSE}_files.list
stats=${out}/${RSE}_stats.json

# X509 proxy
if [ "$cert" != "" ]; then
        if [ "$key" == "" ]; then
            export X509_USER_PROXY=$cert
        else
            voms-proxy-init -voms cms -rfc -valid 192:00 --cert $cert --key $key
        fi
fi

export PYTHONPATH=`pwd`/cmp3

$python scanner/xrootd_scanner.py -o ${file_list_prefix} -n 1 -R 1 -z -c ${config_file} -s ${stats} ${RSE} 
if [ "$?" != "0" ]; then
	rm -f ${stats} ${file_list_prefix}* 
        echo "Site scan failed. Exiting"
	exit 1
fi
