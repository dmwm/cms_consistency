#!/bin/bash


#
# Usage:
#   RAL_compare.sh <config.yaml> <dbconfig.cfg> <RSE> <scratch dir> <output dir> [<cert file> [<key file>]]
#

config=$1
rucio_config_file=$2
RSE=$3
scratch=$4
out=$5
cert=$6
key=$7

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


sleep_interval=1000      # 10 minutes
attempts="1 2 3 4 5 6"


run=`date -u +%Y_%m_%d_00_00`
timestamp=`date -u +%Y%m%d`
dump_url=root://${server}/cms:${dump_path}/dump_${timestamp}.gz

b_prefix=${scratch}/${RSE}_${run}_B.list
a_prefix=${scratch}/${RSE}_${run}_A.list
r_prefix=${scratch}/${RSE}_${run}_R.list
stats=${out}/${RSE}_${run}_stats.json

d_out=${out}/${RSE}_${run}_D.list
m_out=${out}/${RSE}_${run}_M.list

tape_dump_tmp=${scratch}/${RSE}_${run}_tape_dump.gz

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
    xrdcp ${dump_url} ${tape_dump_tmp}
    xrdcp_status=$?
    if [ "$xrdcp_status" != "0" ] || [ ! -f ${tape_dump_tmp} ]; then
	    rm -f ${tape_dump_tmp}
    	t1=`date +%s`
		python3 cmp3/stats.py -k scanner ${stats} <<_EOF_
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
		
		python3 cmp3/stats.py -k scanner ${stats} <<_EOF_
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

        break
    fi
done

if [ "$downloaded" == "no" ]; then
	python3 cmp3/stats.py -k scanner ${stats} <<_EOF_
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

    

