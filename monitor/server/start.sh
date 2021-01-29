#!/bin/bash

echo start.sh: version 1

CC_DATA=/cc_data
WM_DATA=/wm_data

cd ~
echo "--- starting ---"
ls -l
ls -l app
ls -ld $DATA
python -V
echo "--- starting server with: " python app/server.py "$@" 8400 $CC_DATA $WM_DATA
python app/server.py "$@" 8400 $CC_DATA $WM_DATA
