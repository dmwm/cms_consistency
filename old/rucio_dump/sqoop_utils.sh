function exit_on_failure()
{
	OUTPUT_ERROR=`cat $TMP_OUT | egrep "ERROR tool.ImportTool: Error during import: Import job failed!"`
        TRANSF_INFO=`cat $TMP_ERR | egrep "mapreduce.ImportJobBase: Transferred"`

	if [[ $OUTPUT_ERROR == *"ERROR"* || ! $TRANSF_INFO == *"INFO"* ]]
    then
	   echo "Error occured, check $LOG_FILE"
	   ./send_email.sh $LOG_FILE.stdout $SCHEMA $START_DATE
	   ./send_email.sh $LOG_FILE.stderr $SCHEMA $START_DATE

       if hdfs dfs -test -e "$BASE_PATH/new"
       then
          hdfs dfs -rm -r $BASE_PATH/new >/dev/null 2>&1
	   fi
	   exit 1
    fi
}

function clean()
{
    if hdfs dfs -test -e "$BASE_PATH/new"
    then
       hdfs dfs -rm -r $BASE_PATH/new >/dev/null 2>&1
	   echo "Removing old $BASE_PATH/new" >> $LOG_FILE.cron
    fi
}

function deploy()
{
    if hdfs dfs -test -e "$BASE_PATH/old"
    then 
       hdfs dfs -rm -r $BASE_PATH/old >/dev/null 2>&1
    fi
    if hdfs dfs -test -e "$BASE_PATH/current"
    then
       hdfs dfs -mv $BASE_PATH/current $BASE_PATH/old
    fi
    hdfs dfs -mv $BASE_PATH/new $BASE_PATH/current
}

function import_table()
{
   kinit -R
   mkdir -p log
   TABLE=$1
   TMP_OUT=log/$TABLE.stdout
   TMP_ERR=log/$TABLE.stderr

   OUTPUT_FOLDER=$BASE_PATH/new/$TABLE
   Q="SELECT * FROM ${SCHEMA}.$TABLE F where \$CONDITIONS"
   echo "Timerange: $START_DATE to $END_DATE" >> $LOG_FILE.cron
   echo "Folder: $OUTPUT_FOLDER" >> $LOG_FILE.cron
   echo "quering...$Q" >> $LOG_FILE.cron

   sqoop import --direct --connect $JDBC_URL --fetch-size 10000 --username $ORACLE_USER --password b1LunDEr11headedcH97Ioc_7ta  --target-dir $OUTPUT_FOLDER -m 1 --query "$Q" \
   --fields-terminated-by , --escaped-by \\ --optionally-enclosed-by '\"' \
   1>$TMP_OUT 2>$TMP_ERR
   cat $TMP_OUT >>$LOG_FILE.stdout
   cat $TMP_ERR >>$LOG_FILE.stderr
   EXIT_STATUS=$?
   exit_on_failure

}

function generateCountQuery
{
	TABS=$1
	i=0
	QUERY=""
	for T in $TABS
	do
      
	  if [ $i -ne 0 ]
	  then 
	    QUERY="$QUERY union all "   
	  fi
	  i=$((i+1))
      QUERY="$QUERY select '$T', count (*) from $SCHEMA.$T"

	done
	QUERY="$QUERY where \$CONDITIONS"
}

function import_tables()
{
   for TABLE_NAME in $1
   do
      import_table $TABLE_NAME
   done
}

function import_counts()
{
   mkdir -p log
   TABS=$1
   generateCountQuery "$TABS"

   OUTPUT_FOLDER=$BASE_PATH/new/ROW_COUNT
   echo "Timerange: $START_DATE to $END_DATE" >> $LOG_FILE.cron
   echo "Folder: $OUTPUT_FOLDER" >> $LOG_FILE.cron
   echo "quering...$QUERY" >> $LOG_FILE.cron

   sqoop import --direct --connect $JDBC_URL --fetch-size 10000 --username $ORACLE_USER --password-file $DBCRED --target-dir $OUTPUT_FOLDER -m 1 --query "$QUERY" \
   --fields-terminated-by , --escaped-by \\ --optionally-enclosed-by '\"' \
   1>>$LOG_FILE.stdout 2>>$LOG_FILE.stderr

   exit_on_failure

}
