#!/bin/sh

version="3.2"

echo site_cmp3 version: $version

if [ "$1" == "" ]; then
	echo 'Usage: site_ce.sh <config file> (<rucio.cfg>|-) <RSE name> <scratch dir> <out dir> [<cert file> [<key file>]]'
	exit 2
fi


config_file=$1
RSE=$3
scratch=$4
out=$5
cert=$6
key=$7

echo "config_file:               $config_file"
echo "RSE:                       $RSE"
echo "scratch:                   $scratch"
echo "out:                       $out"
echo "cert:                      $cert"
echo "key:                       $key"

if [ ! -f /consistency/config.yaml ]; then
    cp $config_file /consistency/config.yaml    # to make it editable
    echo Config file $config_file copied to /consistency/config.yaml
fi

config_file=/consistency/config.yaml

python=${PYTHON:-python}

export PYTHONPATH=`pwd`:`pwd`/cmp3

echo will use python: $python

if [ ! -d ${scratch} ] && [ ! -L ${scratch} ]; then
        mkdir -p ${scratch}
	if [ $? ]; then
		echo Scratch directory does not exist and can not be created
		exit 1
	fi
fi

now=`date -u +%Y_%m_%d_%H_%M`
timestamp=`date -u +%s`

stats=${out}/${RSE}_${now}_edstats.json
scanner_errors=${out}/${RSE}_${now}_scanner.errors

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
    "rse":          "${RSE}",
    "scratch":      "${scratch}",
    "out":          "${out}",
    "config":       "${config_file}",
    "python_version":   "${python_version}",
    "end_time":     null
}
_EOF_


# 2. Site dump
echo
echo Site dump ...
echo

r_prefix=${scratch}/${RSE}_R
rm -f ${r_prefix}.*

empty_dirs_out=${out}/${RSE}_${now}_ED.list

echo "Site scan..." > ${scanner_errors}
$python xrootd/xrootd_scanner.py -z -c ${config_file} -s ${stats} \
    -o ${r_prefix} \
    -e ${empty_dirs_out} \
    ${RSE} 2>> ${scanner_errors}
scanner_status=$?
if [ "$scanner_status" != "0" ]; then
    echo "Site scan failed. Status code: $scanner_status" >> ${scanner_errors}
	rm -f ${r_prefix}*
	exit 1
fi
        
#
# 6. Remove empty directories
#
echo
echo Empty directories ...
echo
ed_action_list=${out}/${RSE}_${now}_ED_action.list
ed_action_errors=${out}/${RSE}_${now}_ED_action.errors
$python actions/remove_empty_dirs.py -o $ed_action_list -s $stats -c ${config_file} -L 10000 $out $RSE 2> $ed_action_errors

end_time=`date -u +%s`

$python cmp3/stats.py $stats << _EOF_
{ "end_time":${end_time}.0 }
_EOF_
