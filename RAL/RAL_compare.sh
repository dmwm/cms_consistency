#!/bin/bash

# alias python=python3 ??

#
# Usage:
#   RAL_compare.sh <config.yaml> <dbconfig.cfg> <RSE> <scratch dir> <output dir> [options ...]
#   Options:
#         -c <cert or proxy file> 
#         -k <key file>
#         -u <unmerged config.yaml> <unmerged list output directory>
#

config=$1
rucio_config_file=$2
RSE=$3
scratch=$4
out=$5
key=""
cert=""
unmerged_out_dir=""
unmerged_config=""

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
    *)
	echo Unknown option $1
	exit 1
	;;
    esac
    shift
done    

server=ceph-gw1.gridpp.rl.ac.uk

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
	
export PYTHONPATH=`pwd`/cmp3:`pwd`


sleep_interval=1800      # 30 minutes
attempts="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20"

run=`date -u +%Y_%m_%d_00_00`
timestamp=`date -u +%Y%m%d`
echo run: $run
echo timestamp: $timestamp

dump_url=root://${server}/cms:${dump_path}/dump_${timestamp}.gz

# HACK
# dump_url=root://ceph-gw1.gridpp.rl.ac.uk/cms:/store/accounting/dump_20210730.gz

echo dump_url: $dump_url

b_prefix=${scratch}/${RSE}_${run}_B.list
a_prefix=${scratch}/${RSE}_${run}_A.list
r_prefix=${scratch}/${RSE}_${run}_R.list
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

python3 cmp3/json_file.py -c $stats set scanner - < $stats_update
if [ "$um_stats" != "" ]; then
    python3 cmp3/json_file.py -c $um_stats set scanner - < $stats_update
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
        
        python3 cmp3/json_file.py ${stats} set scanner.scanner.attempt $attempt
        python3 cmp3/json_file.py ${stats} set scanner.scanner.status -t failed
        python3 cmp3/json_file.py ${stats} set scanner.scanner.last_attempt_time_utc -t "$attempt_time"
        python3 cmp3/json_file.py ${stats} set scanner.scanner.status_code $xrdcp_status
        python3 cmp3/json_file.py ${stats} set scanner.scanner.stderr -t - < $stderr
	
        echo download failed:
        cat $stderr
	    echo
        echo sleeping $sleep_interval ...
        sleep $sleep_interval
    else
        echo download succeeded
        echo partitioning ...
        n=`python3 cmp3/partition.py -c $config -r $RSE -q -o ${r_prefix} ${site_dump_tmp}`
	    echo $n files in the list


    	t1=`date +%s`
        downloaded="yes"

        python3 cmp3/json_file.py ${stats} set scanner.scanner.attempt $attempt
        python3 cmp3/json_file.py ${stats} set scanner.scanner.status -t "done"
        python3 cmp3/json_file.py ${stats} set scanner.scanner.last_attempt_time_utc -t "$attempt_time"
        python3 cmp3/json_file.py ${stats} set scanner.scanner.status_code $xrdcp_status
        python3 cmp3/json_file.py ${stats} set scanner.scanner.stderr -t ""

        python3 cmp3/json_file.py ${stats} set scanner.total_files $n
        
        
        # unmerged files list and stats
        if [ "$unmerged_out_dir" != "" ]; then
            echo making unmerged files list ...
            n=`python3 cmp3/partition.py -c $unmerged_config -r $RSE -z -q -n 1 -o $um_list_prefix $site_dump_tmp`
	        echo $n files in the list

            if [ "$um_stats" != "" ]; then
                python3 cmp3/json_file.py $um_stats set scanner.files $n
            fi   
        fi
        break
    fi
done

rm -f $site_dump_tmp

if [ "$downloaded" == "yes" ]; then
    python3 cmp3/json_file.py ${stats} set scanner.status -t "done"
    python3 cmp3/json_file.py ${stats} set scanner.end_time $t1
    if [ "$um_stats" != "" ]; then
        python3 cmp3/json_file.py $um_stats set scanner.status -t "done"
        python3 cmp3/json_file.py $um_stats set scanner.end_time $t1
    fi
else
    python3 cmp3/json_file.py ${stats} set scanner.status -t "failed"
    python3 cmp3/json_file.py ${stats} set scanner.end_time $t1

    if [ "$um_stats" != "" ]; then
        python3 cmp3/json_file.py $um_stats set scanner.status -t "failed"
        python3 cmp3/json_file.py $um_stats set scanner.end_time $t1
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

python3 cmp3/db_dump.py -o ${a_prefix} -c ${config} $rucio_cfg -s ${stats} -S "dbdump_after" ${RSE} 
                
echo
echo Comparing ...
echo

python3 cmp3/cmp3.py -s ${stats} ${b_prefix} ${r_prefix} ${a_prefix} ${d_out} ${m_out}

echo Dark list:    `wc -l ${d_out}`
echo Missing list: `wc -l ${m_out}`

    

