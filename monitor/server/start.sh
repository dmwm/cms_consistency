#!/bin/bash

echo start.sh: version 1

DATA=/reports

cd ~
echo "--- starting ---"
ls -l
ls -l app
ls -ld $DATA
ls -l $DATA
python -V
python app/server.py "$@" 8400 $DATA
