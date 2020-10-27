#!/bin/sh

version="1.3"

echo site_cmp3 version: $version

if [ "$1" == "" ]; then
	echo 'Usage: site_cmp3.sh <config file> <rucio.cfg> <RSE name> <scratch dir> <out dir> [<cert file> [<key file>]]'
	exit 2
fi


config_file=$1
rucio_config_file=$2
RSE=$3
scratch=$4
out=$5
cert=$6
key=$7

echo "config_file:               $config_file"
echo "rucio_config_file:         $rucio_config_file"
echo "RSE:                       $RSE"
echo "scratch:                   $scratch"
echo "out:                       $out"
echo "cert:                      $cert"
echo "key:                       $key"

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

now=`date +%Y_%m_%d_%H_%M`

d_out=${out}/${RSE}_${now}_D.list
m_out=${out}/${RSE}_${now}_M.list
stats=${out}/${RSE}_${now}_stats.json

# X509 proxy
if [ "$cert" != "" ]; then
        if [ "$key" == "" ]; then
            export X509_USER_PROXY=$cert
        else
            voms-proxy-init -voms cms -rfc -valid 192:00 --cert $cert --key $key
        fi
fi



# 0. delete old lists
rm -f ${a_prefix}*
rm -f ${b_prefix}*
rm -f ${r_prefix}*

# 1. DB dump "before"
echo
echo DB dump before ...
echo

$python db_dump.py -o ${b_prefix} -d ${rucio_config_file} -c ${config_file} ${RSE} 
#ls -l ${b_prefix}*
sleep 10

# 2. Site dump
echo
echo Site dump ...
echo

$python xrootd_scanner.py -o ${r_prefix} -c ${config_file} -s ${stats} ${RSE} 
if [ "$?" != "0" ]; then
	rm -f ${r_prefix}*
        echo "Site scan failed. Exiting"
	exit 1
fi
        
#ls -l ${r_prefix}*
sleep 10

# 3. DB dump "after"
echo
echo DB dump after ...
echo

$python db_dump.py -o ${a_prefix} -d ${rucio_config_file} -c ${config_file} ${RSE} 

# 4. cmp3

echo
echo Comparing ...
echo

$python cmp3.py ${b_prefix} ${r_prefix} ${a_prefix} ${d_out} ${m_out}

echo Dark list:    `wc -l ${d_out}`
echo Missing list: `wc -l ${m_out}`



