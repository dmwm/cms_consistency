#!/bin/sh

if [ "$1" == "" ]; then
	echo 'Usage: run_db_dump.sh <config file> <RSE name> <output dir>'
	exit 2
fi

config_file=$1
RSE=$2
output=$3

mkdir -p ${output}

if [ ! -d ${output} ]; then
	echo Output must be a directory
	exit 1
fi

cfg_dir=/tmp/${USER}/recon_dbdump

mkdir -p $cfg_dir
chmod go-rwx $cfg_dir
cp $config_file ${cfg_dir}/config.json
chmod go-rwx ${cfg_dir}/config.json

docker run --rm -v ${cfg_dir}:/config -v ${output}:/out cms-recon \
	python replicas_for_rse.py -o /out/${RSE}.dbdump -n 10 -c /config/config.json ${RSE} 
