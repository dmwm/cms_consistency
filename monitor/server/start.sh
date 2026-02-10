#!/bin/bash

echo start.sh: version 2

CC_DATA=/reports
WM_DATA=/reports/unmerged
PATH=/app/.venv/bin:$PATH

echo "--- starting ---"
cd /app

python -V

echo "--- starting server with: " python app/server.py --um-ignore /store/unmerged/logs/ -p 8400 "$@" $CC_DATA $WM_DATA
python app/server.py --um-ignore /store/unmerged/logs/ -p 8400 "$@" $CC_DATA $WM_DATA
