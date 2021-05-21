#!/bin/sh

version="1.3"

echo site_cmp3 version: $version

if [ "$1" == "" ]; then
	echo 'Usage: site_cmp3.sh <config file> (<rucio.cfg>|-) <RSE name> <scratch dir> <out dir> [<cert file> [<key file>]]'
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

export PYTHONPATH=`pwd`/cmp3:`pwd`

echo will use python: $python

mkdir -p ${scratch}
if [ ! -d ${scratch} ]; then
	echo Scratch directory does not exist and can not be created
	exit 1
fi

a_prefix=${scratch}/${RSE}_A.list
b_prefix=${scratch}/${RSE}_B.list
r_prefix=${scratch}/${RSE}_R.list

now=`date -u +%Y_%m_%d_%H_%M`
timestamp=`date -u +%s`

d_out=${out}/${RSE}_${now}_D.list.gz
m_out=${out}/${RSE}_${now}_M.list.gz
stats=${out}/${RSE}_${now}_stats.json

# X509 proxy
if [ "$cert" != "" ]; then
        if [ "$key" == "" ]; then
            export X509_USER_PROXY=$cert
        else
            voms-proxy-init -voms cms -rfc -valid 192:00 --cert $cert --key $key
        fi
fi

# init stats file
now_date_time=`date -u`
python_version=`$python -V`
cat > ${stats} <<_EOF_
{
    "start_time":   ${timestamp}.0,
    "start_date_time_utc":  "${now_date_time}",
    "run":          "${now}",
    "rse":          "${RSE}",
    "scratch":      "${scratch}",
    "out":          "${out}",
    "config":       "${config_file}",
    "rucio_config": "${rucio_config_file}",
    "driver_version":   "${version}",
    "python_version":   "${python_version}",
    "end_time":     null
}
_EOF_


# 0. delete old lists
rm -f ${a_prefix}*
rm -f ${b_prefix}*
rm -f ${r_prefix}*

# 1. DB dump "before"
echo
echo DB dump before ...
echo

rucio_cfg=""
if [ "$rucio_config_file" != "-" ]; then
    rucio_cfg="-d $rucio_config_file"
fi

$python cmp3/db_dump.py -o ${b_prefix} -c ${config_file} $rucio_cfg -s ${stats} -S "dbdump_before" ${RSE} 

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

$python cmp3/db_dump.py -o ${a_prefix} -c ${config_file} $rucio_cfg -s ${stats} -S "dbdump_after" ${RSE} 

# 4. cmp3

echo
echo Comparing ...
echo

$python cmp3/cmp3.py -z -s ${stats} ${b_prefix} ${r_prefix} ${a_prefix} ${d_out} ${m_out}

echo "Dark list:    " `gunzip ${d_out} | wc -l`
echo "Missing list: " `gunzip ${m_out} | wc -l`

end_time=`date -u +%s`

$python cmp3/stats.py stats.json << _EOF_
{ "end_time":${end_time}.0 }
_EOF_



