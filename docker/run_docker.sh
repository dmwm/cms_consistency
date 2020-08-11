#!/bin/sh

if [ "$1" == "" ]; then
	echo 'Usage: run_site_cmp3.sh [--shell] <config file> <rucio.cfg> <RSE name> <output dir> <cert> <key>'
	exit 2
fi

shell=""

if [ "$1" == "--shell" ]; then
	shell="--shell"
	shift
fi

config_file=$1
rucio_config_file=$2
RSE=$3
output=$4
cert=$5
key=$6

mkdir -p ${output}
if [ ! -d ${output} ]; then
	echo Output must be a directory
	exit 1
fi

scratch=/tmp/${USER}/cms_recon
mkdir -p $scratch
if [ ! -d ${scratch} ]; then
        echo Scartch must be a directory
        exit 1
fi

cfg_dir=${scratch}

mkdir -p $output
mkdir -p $cfg_dir
chmod go-rwx $cfg_dir
cp $config_file ${cfg_dir}/config.yaml
cp $rucio_config_file ${cfg_dir}/rucio.cfg
cp $cert ${cfg_dir}/cert
cp $key ${cfg_dir}/key
chmod go-rwx ${cfg_dir}/*

if [ "$shell" == "--shell" ]; then
        echo would run: ./run.sh $shell /config ${RSE} /output /config/cert /config/key
	docker run -ti --rm -v ${cfg_dir}:/config -v ${output}:/output cms-recon /bin/bash
else
	docker run --rm -v ${cfg_dir}:/config -v ${output}:/output cms-recon \
		   ./run.sh $shell /config ${RSE} /output /config/cert /config/key
fi




