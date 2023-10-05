#!/bin/bash

action=$1
rse=$2
disabled_command=CE_config.disabled

case $action in
disable)
	rucio-admin rse set-attribute --rse $rse --key $disabled_command --value true
  ;;
enable)
	rucio-admin rse set-attribute --rse $rse --key $disabled_command --value false
  ;;
show)
  rucio-admin rse get-attribute $rse | grep -e ^${disabled_command}:
  ;;
*)
  echo Unknown action: $action
  exit 1
  ;;
esac