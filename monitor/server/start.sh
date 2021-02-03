#!/bin/bash

echo start.sh: version 2

CC_DATA=/reports
WM_DATA=/reports/unmerged

cd ~
echo "--- starting ---"
ls -l
ls -l app

echo "--- $CC_DATA: ---"
ls -ld $CC_DATA
ls -l $CC_DATA

echo "--- $WM_DATA: ---"
ls -ld $WM_DATA
ls -l $WM_DATA

python -V

echo "--- starting server with: " python app/server.py "$@" 8400 $CC_DATA $WM_DATA
python app/server.py "$@" 8400 $CC_DATA $WM_DATA
