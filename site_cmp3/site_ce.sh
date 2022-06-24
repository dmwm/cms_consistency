#!/bin/sh

version="2.0"

echo site_cmp3 version: $version

if [ "$1" == "" ]; then
	echo 'Usage: site_ce.sh <config file> (<rucio.cfg>|-) <scope> <RSE name> <scratch dir> <out dir> [<cert file> [<key file>]]'
	exit 2
fi


config_file=$1
rucio_config_file=$2
scope=$3
RSE=$4
scratch=$5
out=$6
cert=$7
key=$8

echo "config_file:               $config_file"
echo "rucio_config_file:         $rucio_config_file"
echo "scope:                     $scope"
echo "RSE:                       $RSE"
echo "scratch:                   $scratch"
echo "out:                       $out"
echo "cert:                      $cert"
echo "key:                       $key"

python=${PYTHON:-python}

export PYTHONPATH=`pwd`/cmp3:`pwd`

echo will use python: $python

if [ ! -d ${scratch} ] && [ ! -L ${scratch} ]; then
        mkdir -p ${scratch}
	if [ $? ]; then
		echo Scratch directory does not exist and can not be created
		exit 1
	fi
fi

a_prefix=${scratch}/${RSE}_A
b_prefix=${scratch}/${RSE}_B
am_prefix=${a_prefix}_M
ad_prefix=${a_prefix}_D
bm_prefix=${b_prefix}_M
bd_prefix=${b_prefix}_D
r_prefix=${scratch}/${RSE}_R

now=`date -u +%Y_%m_%d_%H_%M`
timestamp=`date -u +%s`

d_out=${out}/${RSE}_${now}_D.list
m_out=${out}/${RSE}_${now}_M.list
stats=${out}/${RSE}_${now}_stats.json
scanner_errors=${out}/${RSE}_${now}_scanner.errors
dbdump_errors=${out}/${RSE}_${now}_dbdump.errors
missing_action_errors=${out}/${RSE}_${now}_missing_action.errors
dark_action_errors=${out}/${RSE}_${now}_dark_action.errors

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
    "driver_script_version": "$version",
    "start_time":   ${timestamp}.0,
    "start_date_time_utc":  "${now_date_time}",
    "run":          "${now}",
    "scope":        "${scope}",
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
rm -f ${am_prefix}.*
rm -f ${ad_prefix}.*
rm -f ${bd_prefix}.*
rm -f ${bm_prefix}.*
rm -f ${r_prefix}.*

# 1. DB dump "before"
echo
echo DB dump before ...
echo

rucio_cfg=""
if [ "$rucio_config_file" != "-" ]; then
    rucio_cfg="-d $rucio_config_file"
fi

echo "DB dump before the scan..." > ${dbdump_errors}
$python cmp3/db_dump.py -z -f A:${bm_prefix} -f "*:${bd_prefix}" -c ${config_file} $rucio_cfg -s ${stats} -S "dbdump_before" ${RSE} 2>> ${dbdump_errors}

sleep 10

# 2. Site dump
echo
echo Site dump ...
echo

echo "Site scan..." > ${scanner_errors}
$python xrootd_scanner.py -z -o ${r_prefix} -c ${config_file} -s ${stats} ${RSE} 2>> ${scanner_errors}
scanner_status=$?
if [ "$scanner_status" != "0" ]; then
    echo "Site scan failed. Status code: $scanner_status" >> ${scanner_errors}
	rm -f ${r_prefix}*
	exit 1
fi
        
#ls -l ${r_prefix}*
sleep 10

# 3. DB dump "after"
echo
echo DB dump after ...
echo

echo "DB dump after the scan..." >> ${dbdump_errors}
$python cmp3/db_dump.py -z -f A:${am_prefix} -f "*:${ad_prefix}" -c ${config_file} $rucio_cfg -s ${stats} -S "dbdump_after" ${RSE} 2>> ${dbdump_errors}

# 4. cmp3

echo
echo Comparing ...
echo

$python cmp3/cmp5.py -s ${stats} \
    ${bm_prefix} ${bd_prefix} \
    ${r_prefix} \
    ${am_prefix} ${ad_prefix} \
    ${d_out} ${m_out}

ndark=`wc -l ${d_out}`
nmissing=`wc -l ${m_out}`

echo "Dark list:    " $ndark
echo "Missing list: " $nmissing

end_time=`date -u +%s`

#
# 5. Declare missing replicas
#
$python actions/declare_missing.py -s $stats $out $scope $rse 2>> ${missing_action_errors}

#
# 5. Declare dark replicas
#
$python actions/declare_dark.py -s $stats $out $rse 2>> ${dark_action_errors}

$python cmp3/stats.py stats.json << _EOF_
{ "end_time":${end_time}.0 }
_EOF_













