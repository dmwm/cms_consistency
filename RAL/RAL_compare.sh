#!/bin/bash


#
# Usage:
#   RAL_compare.sh <config.yaml> <dbconfig.cfg> <RSE> <scratch dir> <output dir> 
#         -c <cert or proxy file> 
#         -k <key file>
#         -u <unmerged list output directory>
#         -U <unmerged root path>           default: /store/unmerged/
#

config=$1
rucio_config_file=$2
RSE=$3
scratch=$4
out=$5
key=""
cert=""
unmerged_out_dir=""
unmerged_path="/store/unmerged/"

# skip required args
shift
shift
shift
shift
shift

while $1; do
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
        unmerged_out_dir=$2
        shift
        ;;
    -U)
        unmerged_path=$2
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
dump_url=root://${server}/cms:${dump_path}/dump_${timestamp}.gz

b_prefix=${scratch}/${RSE}_${run}_B.list
a_prefix=${scratch}/${RSE}_${run}_A.list
r_prefix=${scratch}/${RSE}_${run}_R.list
stats=${out}/${RSE}_${run}_stats.json
stats_update=${scratch}/${RSE}_update.json

d_out=${out}/${RSE}_${run}_D.list
m_out=${out}/${RSE}_${run}_M.list

tape_dump_tmp=${scratch}/${RSE}_${run}_tape_dump.gz

if [ "$unmerged_out_dir" != "" ]; then
    um_stats=${unmerged_out_dir}/${RSE}_${run}_stats.json
    um_list_prefix=${unmerged_out_dir}/${RSE}_files.list
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
echo Downloading tape dump ...
echo

downloaded="no"

t0=`date +%s`

for attempt in $attempts; do
    echo Attempt $attempt ...
    rm -f ${tape_dump_tmp}
    attempt_time=`date -u`
    xrdcp ${dump_url} ${tape_dump_tmp} > ${scratch}/${RSE}_xrdcp_${run}.stderr 2>&1
    xrdcp_status=$?
    if [ "$xrdcp_status" != "0" ] || [ ! -f ${tape_dump_tmp} ]; then
	    rm -f ${tape_dump_tmp}
    	t1=`date +%s`
        cat > $stats_update <<_EOF_
		    {
		        "rse":"$RSE",
		        "scanner":{
		            "type":"site_dump",
					"url":"${dump_url}",
		            "version":null,
                    "last_attempt_time_utc": "${attempt_time}",
    		        "status":   "failed",
                    "status_code": ${xrdcp_status},
    				"attempt":  $attempt
		        },
		        "server":"${server}",
		        "start_time":$t0,
		        "end_time":null,
		        "status":   "running"
		    }
_EOF_
		python3 cmp3/stats.py -k scanner ${stats} < $stats_update
        if [ "$um_stats" != "" ]; then
    		python3 cmp3/stats.py -k scanner ${$um_stats} < $stats_update
        fi

        echo sleeping ...
        sleep $sleep_interval
    else
        echo succeeded
        python3 cmp3/partition.py -c $config -r $RSE -q -o ${r_prefix} ${tape_dump_tmp}
    	t1=`date +%s`
	    rm -f ${tape_dump_tmp}
        downloaded="yes"

		# count files in the dump
		n=`wc -l ${r_prefix}.* | egrep  '^[ ]*[0-9]+[ ]+total' | awk -e '{ print $1 }'`
		n=${n:-0}
		
        cat > $stats_update <<_EOF_
		    {
		        "rse":"$RSE",
		        "scanner":{
		            "type":"site_dump",
					"url":"${dump_url}",
		            "version":null,
                    "last_attempt_time_utc": "${attempt_time}",
    		        "status":   "done",
    				"attempt":  $attempt
		        },
		        "server":"${server}",
		        "start_time":$t0,
		        "end_time":$t1,
		        "status":   "done",
				"total_files":$n
		    }
_EOF_
        python3 cmp3/stats.py -k scanner ${stats} < $stats_update
        # unmerged files list and stats
        if [ "$unmerged_out_dir" != "" ]; then
            grep ^${unmerged_path} ${tape_dump_tmp} > ${um_list_prefix}
    		n=`wc -l ${um_list_prefix} | egrep  '^[ ]*[0-9]+[ ]+total' | awk -e '{ print $1 }'`
    		n=${n:-0}
            gzip ${um_list_prefix}
    		cat > ${um_stats} <<_EOF_
		    {
                "scanner": {
    		        "rse":"$RSE",
    		        "scanner":{
    		            "type":"site_dump",
    					"url":"${dump_url}",
    		            "version":null,
                        "last_attempt_time_utc": "${attempt_time}",
        		        "status":   "done",
        				"attempt":  $attempt
    		        },
    		        "server":"${server}",
    		        "start_time":$t0,
    		        "end_time":$t1,
    		        "status":   "done",
    				"total_files":$n
                }
		    }
_EOF_

        break
    fi
done

if [ "$downloaded" == "no" ]; then
	cat > ${scratch}/${RSE}_update.json <<_EOF_
		    {
		        "rse":"$RSE",
		        "scanner":{
		            "type":"site_dump",
					"url":"${dump_url}",
		            "version":null,
                    "last_attempt_time_utc": "${attempt_time}",
    		        "status":   "failed",
                    "status_code": ${xrdcp_status},
    				"attempt":  $attempt
		        },
		        "server":"${server}",
		        "start_time":$t0,
		        "end_time":$t1,
		        "status":   "failed",
                "last_attempt_time_utc": "${attempt_time}",
				"attempt":$attempt
		    }
_EOF_
    python3 cmp3/stats.py -k scanner ${stats} < ${scratch}/${RSE}_update.json
    if [ "$um_stats" != "" ]; then
		python3 cmp3/stats.py -k scanner ${$um_stats} < ${scratch}/${RSE}_update.json
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

    

