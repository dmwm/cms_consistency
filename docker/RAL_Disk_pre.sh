#!/usr/bin/env bash

# This script should start and finish before the RAL disk dump starts at RAL

cp /opt/proxy/x509up /tmp/x509up
chmod 600 /tmp/x509up
export X509_USER_PROXY=/tmp/x509up

cd /consistency/cms_consistency/RAL

run=`date -u +%Y_%m_%d_%H_%M`
out=/var/cache/consistency-dump/
RSE='T1_UK_RAL_Disk'
merged_config_file=${out}/${RSE}_${run}_config.yaml
config_file='/config/config.yaml'
python=python3

echo "merged_confg_file:         $merged_config_file"

RUCIO_ACCOUNT=transfer_ops $python cmp3/merge_config.py merge $RSE $config_file > $merged_config_file

disabled=`$python cmp3/merge_config.py get -d false $merged_config_file rses.$RSE.ce_disabled`
echo "RSE disabled:              $disabled"
if [[ "$disabled"  =~ ^(true|True)$ ]]; then
    echo \|
    echo \| The CE for RSE is disabled. Stopping
    echo \|
    exit 0
fi

./RAL_dbdump.sh $merged_config_file /opt/rucio/etc/rucio.cfg T1_UK_RAL_Disk /var/cache/consistency-temp/ /var/cache/consistency-dump/
