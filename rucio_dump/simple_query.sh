#!/bin/bash

. $(dirname $0)/sqoop_utils.sh

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
OUTPUT_FOLDER=$BASE_PATH/output

if hdfs dfs -test -e $OUTPUT_FOLDER
then
    hdfs dfs -rm -r $OUTPUT_FOLDER
fi

if [ -n "$1" ]
then
	START_DATE=$1
else
	START_DATE=`date +'%F'`
fi

year=`date +'%Y' -d "$START_DATE"`
month=`date +'%-m' -d "$START_DATE"`
day=`date +'%-d' -d "$START_DATE"`

export START_DATE_S=`date +'%s' -d "$START_DATE"`
export LOG_FILE=log/`date +'%F_%H%m%S'`_`basename $0`



Q="SELECT ${SCHEMA}.rses.* FROM ${SCHEMA}.rses where \$CONDITIONS"
echo Q=$Q


clean



sqoop import --direct --connect $JDBC_URL --fetch-size 10000 \
	--username $ORACLE_USER --password-file $DBCRED \
	--target-dir $OUTPUT_FOLDER -m 1 \
	--query "$Q" \
	--fields-terminated-by , --escaped-by \\ --optionally-enclosed-by '\"' \


deploy
