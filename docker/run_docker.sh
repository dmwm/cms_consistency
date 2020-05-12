#!/bin/sh

if [ "$1" == "" ]; then
	echo 'Usage: run_db_dump.sh <config file> <RSE name> <output dir>'
	exit 2
fi

config_file=$1
RSE=$2
output=$3

docker run --rm -v ${config_file}:/config.json -v ${output}:/out cms-recon /home/rucio/dump_site.sh ${RSE} ${output}
