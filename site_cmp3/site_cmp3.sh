#!/bin/sh

version="4.0"

echo site_cmp3 version: $version

if [ "$1" == "" ]; then
	echo 'Usage: site_ce.sh <config file> (<rucio.cfg>|-) <RSE name> <scratch dir> <out dir> [<cert file> [<key file>]]'
	exit 2
fi

now=`date -u +%Y_%m_%d_%H_%M`
run=$now
timestamp=`date -u +%s`

config_file=$1
rucio_config_file=$2
RSE=$3
scratch=$4
out=$5
cert=$6
key=$7
scope=cms

echo "config_file:               $config_file"
echo "rucio_config_file:         $rucio_config_file"
echo "scope:                     $scope"
echo "RSE:                       $RSE"
echo "scratch:                   $scratch"
echo "out:                       $out"
echo "cert:                      $cert"
echo "key:                       $key"

python=${PYTHON:-python}

export PYTHONPATH=`pwd`:`pwd`/cmp3

echo "python:                    $python"

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

d_out=${out}/${RSE}_${now}_D.list
m_out=${out}/${RSE}_${now}_M.list
stats=${out}/${RSE}_${now}_stats.json
scanner_errors=${out}/${RSE}_${now}_scanner.errors
dbdump_errors=${out}/${RSE}_${now}_dbdump.errors
root_file_counts=${out}/${RSE}_${now}_root_file_counts.json

# X509 proxy
if [ "$cert" != "" ]; then
        if [ "$key" == "" ]; then
            export X509_USER_PROXY=$cert
        else
            voms-proxy-init -voms cms -rfc -valid 192:00 --cert $cert --key $key
        fi
fi

merged_config_file=${out}/${RSE}_${run}_config.yaml

echo "merged_confg_file:         $merged_config_file"

$python merge_config.py merge $RSE $config_file > $merged_config_file
disabled=`$python merge_config.py get -d false $merged_config_file rses.$RSE.ce_disabled`

case $disabled in
  True|true)
    disabled="true"
    ;;
  False|false)
    disabled="false"
    ;;
  *)
    disabled="false"
    ;;
esac

echo "RSE disabled:              $disabled"

# init stats file
now_date_time=`date -u`
python_version=`$python -V`
cat > ${stats} <<_EOF_
{
    "driver_script_version": "$version",
    "start_time":   ${timestamp}.0,
    "start_date_time_utc":  "${now_date_time}",
    "run":          "${run}",
    "scope":        "${scope}",
    "rse":          "${RSE}",
    "scratch":      "${scratch}",
    "out":          "${out}",
    "config":       "${config_file}",
    "merged_config":      "${merged_config_file}",
    "rucio_config":       "${rucio_config_file}",
    "driver_version":     "${version}",
    "python_version":     "${python_version}",
    "end_time":     ${timestamp}.0,
    "elapsed_time":  ${timestamp}.0,
    "disabled":     $disabled
}
_EOF_


if [ "$disabled" == "true" ]; then
    echo \|
    echo \| The CE for RSE is disabled. Stopping
    echo \|
    exit 0
fi


# 0. delete old lists
rm -f ${am_prefix}.*
rm -f ${ad_prefix}.*
rm -f ${bd_prefix}.*
rm -f ${bm_prefix}.*

# 1. DB dump "before"
echo
echo DB dump before ...
echo

rucio_cfg=""
if [ "$rucio_config_file" != "-" ]; then
    rucio_cfg="-d $rucio_config_file"
fi

echo "DB dump before the scan..." > ${dbdump_errors}
rce_db_dump -z -c ${merged_config_file} $rucio_cfg \
    -f A:${bm_prefix} -f "*:${bd_prefix}" \
    -s ${stats} -S "dbdump_before" \
    -r $root_file_counts \
    ${RSE} 2>> ${dbdump_errors}

sleep 10

# 2. Site dump
echo
echo Site dump ...
echo

r_prefix=${scratch}/${RSE}_R
rm -f ${r_prefix}.*

empty_dirs_out=${out}/${RSE}_${now}_ED.list.gz

echo "Site scan..." > ${scanner_errors}
rce_scan -z -c ${merged_config_file} -s ${stats} \
    -o ${r_prefix} \
    -r $root_file_counts \
    -E 1 -e $empty_dirs_out \
    ${RSE} 2>> ${scanner_errors}
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
rce_db_dump -z -c ${merged_config_file} $rucio_cfg -f A:${am_prefix} -f "*:${ad_prefix}" -s ${stats} -S "dbdump_after" ${RSE} 2>> ${dbdump_errors}

# 4. cmp3

echo
echo Comparing ...
echo

rce_cmp5 -z -s ${stats} \
    ${bm_prefix} ${bd_prefix} \
    ${r_prefix} \
    ${am_prefix} ${ad_prefix} \
    ${d_out} ${m_out}

# 4.1 Calculate diffs with previous run
$python cmp3/diffs.py -u -s ${stats} $out $RSE $now

#
# 5. Declare missing and dark replicas
#    -d turns it into "dry run" mode
#
echo
echo Missing files ...
echo
missing_action_errors=${out}/${RSE}_${now}_missing_action.errors
m_action_list=${out}/${RSE}_${now}_M_action.list
$python actions/declare_missing.py -a root -o ${m_action_list} -c ${merged_config_file} -s $stats $out $scope $RSE 2>> ${missing_action_errors}

echo
echo Dark files ...
echo
d_action_list=${out}/${RSE}_${now}_D_action.list
dark_action_errors=${out}/${RSE}_${now}_dark_action.errors
$python actions/declare_dark.py -a root -o ${d_action_list} -c ${merged_config_file} -s $stats $out $RSE 2>> ${dark_action_errors}

#
# 6. Remove empty directories
#
echo
echo Empty directories ...
echo
ed_action_list=${out}/${RSE}_${now}_ED_action.list
ed_action_errors=${out}/${RSE}_${now}_ED_action.errors
$python actions/remove_empty_dirs.py -s $stats -c ${merged_config_file} -L 10000 $out $RSE 2> $ed_action_errors

end_time=`date -u +%s`
elapsed_time=$((end_time - timestamp))

rce_update_stats $stats << _EOF_
{ 
    "end_time": ${end_time}.0,
    "elapsed_time": ${elapsed_time}.0
}
_EOF_

#
# 7. Push2Prometheus
#
echo
echo Pushing stats...
echo
$python /consistency/push2prometheus.py $stats "site_cmp3"
