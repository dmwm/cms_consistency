#!/bin/bash +x
# file: site-scan-test.sh

#.. check if sourcing or running from a subprocess
shelltag=`echo $0 | grep bash`
if [ $shelltag"x" == "x" ]; then
  cmd=$0
  myexit=exit
else
  cmd=./site-scan-test.sh
  myexit=return
fi

if [[ $# -lt 1 ]]; then
  echo ""
  echo "Usage: ${cmd} <RSE>"
  $myexit
fi

#.. input
RSE=$1
dump=/var/cache/consistency-dump
temp=/var/cache/consistency-temp

#.. output
out=/var/cache/test

ls -t1 ${dump}/${RSE}*stats.json | sed 's#_stats.json##' | sed "s#${dump}/${RSE}_##" > ${out}/${RSE}-dates.out
#len=$(expr length "${RSE}_")

last=`head -1 ${out}/${RSE}-dates.out`
now=`date -u +%Y_%m_%d_%H_%M`

r_prefix=${out}/${RSE}_R
root_file_counts=${dump}/${RSE}_${last}_root_file_counts.json
scanner_errors=${out}/${RSE}_${now}_errors.out
empty_dirs=${out}/${RSE}_${now}_ED.out

rce_scan -z -t 10 \
  -c ${dump}/${RSE}_${last}_config.yaml \
  -s ${out}/${RSE}_${now}_stats.json \
  -o ${r_prefix} \
  -r ${root_file_counts} \
  -E 2 -e ${empty_dirs} \
  ${RSE} > ${scanner_errors}
