#!/bin/sh

version="1.3"

echo site_cmp3 version: $version

if [ "$1" == "" ]; then
	echo 'Usage: site_cmp3.sh <config dir> <RSE name> <scratch dir> [<cert file> [<key file>]]'
	exit 2
fi

cfgdir=$1
config_file=${cfgdir}/config.yaml
rucio_config_file=${cfgdir}/rucio.cfg
RSE=$2
scratch=$3
cert=$4
key=$5

python=${PYTHON:-python}

echo will use python: $python

mkdir -p ${scratch}
if [ ! -d ${scratch} ]; then
	echo Scratch directory does not exist and can not be created
	exit 1
fi

a_prefix=${scratch}/${RSE}_A.list
b_prefix=${scratch}/${RSE}_B.list
r_prefix=${scratch}/${RSE}_R.list
d_out=${scratch}/${RSE}_D.list
m_out=${scratch}/${RSE}_M.list

# X509 proxy
if [ "$cert" != "" ]; then
        if [ "$key" == "" ]; then
            export X509_USER_PROXY=$cert
        else
	    voms-proxy-init -voms cms -rfc -valid 192:00 --cert $cert --key $key
        fi
fi


#cd ~/cms_consistency/site_cmp3
#echo git pull...
#git pull

# 1. DB dump "before"
echo
echo DB dump before ...
echo

rm -rf ${b_prefix}*
$python db_dump.py -o ${b_prefix} -d ${rucio_config_file} -c ${config_file} ${RSE} 
#ls -l ${b_prefix}*
sleep 10

# 2. Site dump
echo
echo Site dump ...
echo

rm -rf ${r_prefix}*
$python xrootd_scanner.py -o ${r_prefix} -c ${config_file} ${RSE} 
#ls -l ${r_prefix}*
sleep 10

# 3. DB dump "after"
echo
echo DB dump after ...
echo

rm -rf ${a_prefix}*
$python db_dump.py -o ${a_prefix} -d ${rucio_config_file} -c ${config_file} ${RSE} 

# 4. cmp3

echo
echo Comparing ...
echo

$python cmp3.py ${b_prefix} ${r_prefix} ${a_prefix} ${d_out} ${m_out}

echo Dark list:    `wc -l ${d_out}`
echo Missing list: `wc -l ${m_out}`


