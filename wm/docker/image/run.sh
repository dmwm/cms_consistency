#!/bin/bash -vx

cfg=$1
rse=$2
out=$3
cert=$4
key=$5

#
# copy X.509 cert and key to make sure they have correct owenrship/permissions
#

cp $key /tmp/my_key
cp $cert /tmp/my_cert
chmod go-rwx /tmp/my_key /tmp/my_cert

cd ~rucio/cms_consistency
git pull				# make sure to pick up the latest version

cd wm

PYTHON=python3 \
	./scan.sh $cfg $rse $out /tmp/my_cert /tmp/my_key

