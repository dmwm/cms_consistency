#!/bin/bash

Usage="files_for_rse.sh [-r <rse>] [-s <start date: YYYY-MM-DD>]"

. $(dirname $0)/sqoop_utils.sh

RSE=''
START_DATE=`date +'%F'`

while [ "$1" != "" ]; do
	case $1 in
		-r)
			RSE=$2
			shift
			;;
		-s)
			START_DATE=$2
			shift
			;;
		-\?|-h|-help|help)
			echo $Usage
			exit 2
	esac
	shift
done

echo RSE: ${RSE:-(all)}
#echo Start date: $START_DATE
echo 

here=`cd $(dirname $0); pwd`

# Initial version to dump the RUCIO data same as ATLAS

# source ORACLE database
export JDBC_URL="jdbc:oracle:thin:@int2r-s.cern.ch:10121/int2r_lb.cern.ch"
#export SCHEMA="CMS_RUCIO_TEST"
export SCHEMA="CMS_RUCIO_DEV_ADMIN"
export TABLE="RSES"
export ORACLE_USER="CMS_RUCIO_DEV_READ"
export DBCRED="file://${here}/dbcred"

echo DBCRED=$DBCRED

# target HDFS folder
export BASE_PATH="/user/rucio2hadoop/ivm"
OUTPUT_FOLDER=$BASE_PATH/output/${RSE:-all}

year=`date +'%Y' -d "$START_DATE"`
month=`date +'%-m' -d "$START_DATE"`
day=`date +'%-d' -d "$START_DATE"`

export START_DATE_S=`date +'%s' -d "$START_DATE"`
export LOG_FILE=log/`date +'%F_%H%m%S'`_`basename $0`


Q="SELECT R.rse, F.scope, F.name, F.adler32, F.bytes, F.created_at, F.path, F.updated_at, F.state, F.accessed_at, F.tombstone " 
Q="$Q FROM ${SCHEMA}.rses R, ${SCHEMA}.replicas F"
Q="$Q where F.rse_id=R.id and (\$CONDITIONS)"

if [ "$RSE" != "" ]; then
	Q="$Q and R.rse='$RSE'"
fi

echo Query: $Q

if hdfs dfs -test -e $OUTPUT_FOLDER
then
    hdfs dfs -rm -r $OUTPUT_FOLDER
fi


clean

sqoop import --direct --connect $JDBC_URL --fetch-size 10000 \
	--username $ORACLE_USER --password-file $DBCRED \
	--target-dir $OUTPUT_FOLDER -m 1 \
	--query "$Q" \
	--fields-terminated-by "\t"  --escaped-by \\ --optionally-enclosed-by '\"' \


deploy
