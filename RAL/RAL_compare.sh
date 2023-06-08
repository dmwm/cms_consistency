#!/bin/bash

#
# Usage:
#   RAL_compare.sh <config.yaml> <dbconfig.cfg> <RSE> <scratch dir> <output dir> [options ...]
#   Options:
#         -c <cert or proxy file> 
#         -k <key file>
#         -u <unmerged config.yaml> <unmerged list output directory>
#
#   Debug options:
#         -r <YYYY_MM_DD> CE run timestamp to use
#         -t <YYYYMMDD> timestamp to use for RAL site dump filename
#

config=$1
rucio_config_file=$2
RSE=$3
scope=cms
scratch=$4
out=$5
key=""
cert=""
unmerged_out_dir=""
unmerged_config=""

run=`date -u +%Y_%m_%d_00_00`
timestamp=`date -u +%Y%m%d`

if [ ! -f /consistency/config.yaml ]; then
    cp $config /consistency/config.yaml    # to make it editable
    echo Config file $config copied to /consistency/config.yaml
fi
config=/consistency/config.yaml

python=${PYTHON:-python3}


# skip required args
shift
shift
shift
shift
shift

while [ -n "$1" ]; do
    case $1 in
    -k)
        key=$2
        shift
        ;;
    -c)
        cert=$2
        shift
        ;;
    -u)
        unmerged_config=$2
        unmerged_out_dir=$3
        shift
        shift
        ;;
    -r)
        run=${2}_00_00
        shift
        echo will be using $run for the run timestamp
        ;;
    -t)
        timestamp=$2
        shift
        echo will be using $timestamp for the site dump filename
        ;;
    *)
        echo Unknown option $1
        exit 1
        ;;
    esac
    shift
done    

# - old - server=ceph-gw1.gridpp.rl.ac.uk
server=xrootd.echo.stfc.ac.uk

case $RSE in
	T1_UK_RAL_Tape)
		dump_path="/store/accounting/tape"
		;;
	T1_UK_RAL_Disk)
		dump_path="/store/accounting"
		;;
	*)
		echo Unknown RSE $RSE
		exit 1
		;;
esac

echo Input site dump path: $dump_path

export PYTHONPATH=`pwd`/cmp3:`pwd`


sleep_interval=1800      # 30 minutes
attempts="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20"


# hack
#run=2021_08_27_00_00
#timestamp=20210827

echo run: $run
echo timestamp: $timestamp

dump_url=root://${server}/cms:${dump_path}/dump_${timestamp}.gz

# HACK
# dump_url=root://ceph-gw1.gridpp.rl.ac.uk/cms:/store/accounting/dump_20220902.gz

echo dump_url: $dump_url

b_prefix=${scratch}/${RSE}_${run}_B
a_prefix=${scratch}/${RSE}_${run}_A
r_prefix=${scratch}/${RSE}_${run}_R
am_prefix=${a_prefix}_M
ad_prefix=${a_prefix}_D
bm_prefix=${b_prefix}_M
bd_prefix=${b_prefix}_D
stats=${out}/${RSE}_${run}_stats.json
stats_update=${scratch}/${RSE}_update.json

d_out=${out}/${RSE}_${run}_D.list
m_out=${out}/${RSE}_${run}_M.list

site_dump_tmp=${scratch}/${RSE}_${run}_site_dump.gz

if [ "$unmerged_out_dir" != "" ]; then
    um_stats=${unmerged_out_dir}/${RSE}_${run}_stats.json
    um_list_prefix=${unmerged_out_dir}/${RSE}_files.list
    cat > /tmp/$$.junk << _EOF_
rses: 
    "*":
        partitions: 1
        preprocess:
            filter:     "/store/unmerged/"
            rewrite:
                match:  "([^ ]+).*"
                out:    \1
_EOF_
fi

# X509 proxy
if [ "$cert" != "" ]; then
        if [ "$key" == "" ]; then
            export X509_USER_PROXY=$cert
        else
            voms-proxy-init -voms cms -rfc -valid 192:00 --cert $cert --key $key
        fi
fi

echo
echo Downloading site dump ...
echo

downloaded="no"

t0=`date +%s`

cat > $stats_update << _EOF_
    {
        "rse":"$RSE",
        "scanner":{
            "type":"site_dump",
			"url":"${dump_url}",
            "version":null,
			"attempt":  0
        },
        "server":"${server}",
        "start_time":$t0,
        "end_time":null,
        "status":   "running"
    }
_EOF_

$python cmp3/json_file.py -c $stats set scanner - < $stats_update
if [ "$um_stats" != "" ]; then
    $python cmp3/json_file.py -c $um_stats set scanner - < $stats_update
fi   

for attempt in $attempts; do
    echo Attempt $attempt ...
    rm -f ${site_dump_tmp}
    attempt_time=`date -u`
    stderr=${scratch}/${RSE}_xrdcp_${run}.stderr
    xrdcp ${dump_url} ${site_dump_tmp} 2> $stderr
    xrdcp_status=$?
    if [ "$xrdcp_status" != "0" ] || [ ! -f ${site_dump_tmp} ]; then
	    rm -f ${site_dump_tmp}
    	t1=`date +%s`
        
        $python cmp3/json_file.py ${stats} set scanner.scanner.attempt $attempt
        $python cmp3/json_file.py ${stats} set scanner.scanner.status -t failed
        $python cmp3/json_file.py ${stats} set scanner.scanner.last_attempt_time_utc -t "$attempt_time"
        $python cmp3/json_file.py ${stats} set scanner.scanner.status_code $xrdcp_status
        $python cmp3/json_file.py ${stats} set scanner.scanner.stderr -t - < $stderr
	
        echo download failed:
        cat $stderr
	    echo
        echo sleeping $sleep_interval ...
        sleep $sleep_interval
    else
        echo download succeeded
        echo partitioning ...
        stderr=${out}/${RSE}_${run}_partition.stderr
        n=`rce_partition -c $config -r $RSE -q -o ${r_prefix} ${site_dump_tmp} 2> $stderr`
        echo "$n replicas in the site dump"

    	t1=`date +%s`
        downloaded="yes"

        $python cmp3/json_file.py ${stats} set scanner.scanner.attempt $attempt
        $python cmp3/json_file.py ${stats} set scanner.scanner.status -t "done"
        $python cmp3/json_file.py ${stats} set scanner.scanner.last_attempt_time_utc -t "$attempt_time"
        $python cmp3/json_file.py ${stats} set scanner.scanner.status_code $xrdcp_status
        $python cmp3/json_file.py ${stats} set scanner.scanner.stderr -t ""
        $python cmp3/json_file.py ${stats} set scanner.total_files $n
        
        
        # unmerged files list and stats
        if [ "$unmerged_out_dir" != "" ]; then
            echo making unmerged files list ...
            filtered_unmerged_list=${scratch}/${RSE}_filtered_unmerged
            gunzip -c $site_dump_tmp | grep -v ^/store/unmerged/logs/ > $filtered_unmerged_list
            stderr=${out}/${RSE}_${run}_um_partition.stderr
            n=`rce_partition -c $unmerged_config -r $RSE -z -q -n 1 -o $um_list_prefix $filtered_unmerged_list 2> $stderr`
            echo partitioning status: $?
            echo $n files in the partitioned list

            if [ "$um_stats" != "" ]; then
                $python cmp3/json_file.py $um_stats set scanner.files $n
            fi   
            rm -f $filtered_unmerged_list
        fi
        break
    fi
done

rm -f $site_dump_tmp

if [ "$downloaded" == "yes" ]; then
    $python cmp3/json_file.py ${stats} set scanner.status -t "done"
    $python cmp3/json_file.py ${stats} set scanner.end_time $t1
    if [ "$um_stats" != "" ]; then
        $python cmp3/json_file.py $um_stats set scanner.status -t "done"
        $python cmp3/json_file.py $um_stats set scanner.end_time $t1
    fi
else
    $python cmp3/json_file.py ${stats} set scanner.status -t "failed"
    $python cmp3/json_file.py ${stats} set scanner.end_time $t1

    if [ "$um_stats" != "" ]; then
        $python cmp3/json_file.py $um_stats set scanner.status -t "failed"
        $python cmp3/json_file.py $um_stats set scanner.end_time $t1
    fi
    exit 1
fi

rucio_cfg=""
if [ "$rucio_config_file" != "-" ]; then
    rucio_cfg="-d $rucio_config_file"
fi

echo
echo DB dump after ...
echo

rce_db_dump -z -f A:${am_prefix} -f "*:${ad_prefix}" -c ${config} $rucio_cfg -s ${stats} -S "dbdump_after" ${RSE}
                
echo
echo Comparing ...
echo

rce_cmp5 -s ${stats} \
    ${bm_prefix} ${bd_prefix} \
    ${r_prefix} \
    ${am_prefix} ${ad_prefix} \
    ${d_out} ${m_out}

echo Dark list:    `wc -l ${d_out}`
echo Missing list: `wc -l ${m_out}`

# Calculate diffs with previous run
$python cmp3/diffs.py -u -s ${stats} $out $RSE $run

#
# 5. Declare missing and dark replicas
#    -d turns it into "dry run" mode
#

missing_action_errors=${out}/${RSE}_${run}_missing_action.errors
dark_action_errors=${out}/${RSE}_${run}_dark_action.errors
m_action_list=${out}/${RSE}_${run}_M_action.list
d_action_list=${out}/${RSE}_${run}_D_action.list

$python actions/declare_missing.py -a root -o ${m_action_list} -c ${config} -s $stats $out $scope $RSE 2> $missing_action_errors
$python actions/declare_dark.py    -a root -o ${d_action_list} -c ${config} -s $stats $out        $RSE 2> $dark_action_errors


