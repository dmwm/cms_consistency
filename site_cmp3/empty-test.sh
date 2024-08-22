#!/bin/bash +x
# file: empty-test

#.. check if sourcing or running from a subprocess
shelltag=`echo $0 | grep bash`
if [ $shelltag"x" == "x" ]; then
  cmd=$0
  myexit=exit
else
  cmd=./empty-test
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

ED_errors=${out}/${RSE}_${now}_ED.errors
export PYTHONPATH=/consistency/cms_consistency/cmp3

/usr/bin/python3 actions/remove_empty_dirs_GL.py -d -v \
  -c ${dump}/${RSE}_${last}_config.yaml \
  -s ${out}/${RSE}_${now}_stats.json \
  ${dump} \
  ${RSE} \
  > ${ED_errors}
