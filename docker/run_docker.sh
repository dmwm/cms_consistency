#!/bin/sh

if [ "$1" == "" ]; then
	echo 'Usage: run_site_cmp3.sh <config file> <RSE name> <output dir> <cert> <key>'
	exit 2
fi

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

scratch=/tmp/${USER}/cms_recon
cfg_dir=${scratch}

mkdir -p $output
mkdir -p $cfg_dir
chmod go-rwx $cfg_dir
cp $config_file ${cfg_dir}/config.yaml
cp $cert ${cfg_dir}/cert
cp $key ${cfg_dir}/key
chmod go-rwx ${cfg_dir}/*

docker run --rm -v ${cfg_dir}:/config -v ${output}:/output cms-recon \
	./run.sh /config/config.yaml ${RSE} /output /config/cert /config/key
