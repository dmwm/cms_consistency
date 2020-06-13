#!/bin/sh

if [ "$1" == "" ]; then
	echo 'Usage: run_docker_shell.sh <config file> <scratch> <cert> <key>'
	exit 2
fi

config_file=$1
output=$2
cert=$3
key=$4

scratch=/tmp/${USER}/cms_recon
mkdir -p $scratch
cfg_dir=${scratch}

mkdir -p $cfg_dir
chmod go-rwx $cfg_dir
cp $config_file ${cfg_dir}/config.yaml
cp $cert ${cfg_dir}/cert
cp $key ${cfg_dir}/key
chmod go-rwx ${cfg_dir}/*

docker run --rm -ti -v ${cfg_dir}:/config -v ${output}:/output cms-recon /bin/bash
