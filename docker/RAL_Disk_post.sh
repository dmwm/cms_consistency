#!/usr/bin/env bash

# This script should run some time after the RAL disk dump script finishes at RAL
cp /opt/proxy/x509up /tmp/x509up
chmod 600 /tmp/x509up
export X509_USER_PROXY=/tmp/x509up
cd /consistency/cms_consistency/RAL

# FIXME: Shouldn't this be used in the last line too?
export RUCIO_CONFIG=/consistency/rucio-client.cfg

run=`date -u +%Y_%m_%d_%H_%M`
out=/var/cache/consistency-dump/
RSE='T1_UK_RAL_Disk'
merged_config_file=${out}/${RSE}_${run}_config.yaml
config_file='/config/config.yaml'
python=python3

# FIXME: Need to create a config file for the unmerged area as well
X509_USER_PROXY=/tmp/x509up RUCIO_ACCOUNT=transfer_ops  $python cmp3/merge_config.py merge $RSE $config_file > $merged_config_file

disabled=`$python cmp3/merge_config.py get -d false $merged_config_file rses.$RSE.ce_disabled`
echo "RSE disabled:              $disabled"
if [[ "$disabled"  =~ ^(true|True)$ ]]; then
    echo \|
    echo \| The CE for RSE is disabled. Stopping
    echo \|
    exit 0
fi


./RAL_compare.sh $merged_config_file /opt/rucio/etc/rucio.cfg T1_UK_RAL_Disk /var/cache/consistency-temp/ $out -u /unmerged-config/config.yaml /var/cache/consistency-dump/unmerged/
