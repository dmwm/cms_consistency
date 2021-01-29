#!/bin/sh

if [ "$1" == "" ]; then
	echo 'Usage: run_site_cmp3.sh [--shell] <config file> <RSE name> <output dir> <cert> <key>'
	exit 2
fi

shell=""

if [ "$1" == "--shell" ]; then
	shell="--shell"
	shift
fi

image=cms-wm-scan

config_file=$1
RSE=$2
output=$3
cert=$4
key=$5

mkdir -p ${output}
if [ ! -d ${output} ]; then
	echo Output must be a directory
	exit 1
fi

cfg_dir=/tmp/wm_config
mkdir -p $cfg_dir
chmod go-rwx $cfg_dir
cp $config_file ${cfg_dir}/config.yaml
cp $cert ${cfg_dir}/cert
cp $key ${cfg_dir}/key
chmod go-rwx ${cfg_dir}/*

if [ "$shell" == "--shell" ]; then
        echo would run: ./run.sh $shell /config/config.yaml ${RSE} /output /config/cert /config/key
	docker run -ti --rm -v ${cfg_dir}:/config -v ${output}:/output $image /bin/bash
else
	docker run --rm -v ${cfg_dir}:/config -v ${output}:/output $image \
		   ./run.sh $shell /config/config.yaml ${RSE} /output /config/cert /config/key
fi




