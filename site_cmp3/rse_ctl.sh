#!/bin/bash

action=$1
rse=$2

case $action in
disable)
	rucio-admin rse set-attribute --rse $rse --key CE_config.ce_disabled --value true
  ;;
enable)
	rucio-admin rse set-attribute --rse $rse --key CE_config.ce_disabled --value false
  ;;
show)
  rucio-admin rse get-attribute $rse | grep ^CE_config.ce_disabled:
  ;;
*)
  echo Unknown action: $action
  exit 1
  ;;
esac