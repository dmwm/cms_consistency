#!/bin/bash

site="RAL"
rse="RAL"

cd ~/$site

scratch_dir="..."
dbdump_top_dir="..."
config="config.yaml"
dbconfig="..."

last_scan=`cat last_scan`
d0=`date "+%Y%m%d"`                 # today
d1=`date -v -1d "+%Y%m%d"`          # yesterday
d2=`date -v -2d "+%Y%m%d"`          # the day before yesterday

function download()
{
    d=$1
    src="root://xrootd.echo.stfc.ac.uk//store/accounting/dump_$d"
    xrdcp $src ${scratch_dir}/${rse}_dump_$d_dump
    if [ "$?" != "0" ]; then
        return $?
    fi
    mkdir -p ${scratch_dir}/${rse}_$d
    python partition.py -c $config -r $rse -q -o ${scratch_dir}/${rse}_$d/site
    rm -f ${scratch_dir}/${rse}_${d}_dump
    return $?
}

function download_latest()
{
    for d in $d0 $d1; do
        if [ "$d" <= "$last_scan" ]; then
            echo ""
            return 0
        fi
        download $d
        status="$?"
        if [ "$status" == "0" ]; then
            echo $d
            return 0
        fi
    done
}

downloaded="$(download_latest)"
if [ "$downloaded" == "" ]; then
    echo "No new site dump found"
    exit 0
fi

site_dump_prefix=${scratch_dir}/${rse}_$d/site

dbdump_date="$d1"
if [ "$downloaded" == "$d1" ]; then
    dbdump_date="$d2"
fi

dbdump_dir_b=${dbdump_top_dir}/${rse}_${dbdump_date}

if [ ! -d "$dbdump_dir_b" ];
    echo "DB dump $dbdump_dir_b for date $dbdump_date not found"
    exit 0
fi

dbdump_b_prefix=${dbdump_dir_b}/dbdump

dbdump_dir_a=${dbdump_top_dir}/${rse}_${d0}
mkdir -p $dbdump_dir_a

dbdump_a_prefix=${dbdump_dir_a}/dbdump

python db_dump.py -c $config -d $dbconfig -o $dbdump_a_prefix

d_out=${scratch_dir}/${rse}_D.list
m_out=${scratch_dir}/${rse}_M.list

$python cmp3.py $dbdump_b_prefix ${site_dump_prefix} ${dbdump_a_prefix} ${d_out} ${m_out}


    

