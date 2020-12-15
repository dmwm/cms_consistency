#!/bin/bash

kubectl -n default create secret generic consistency-monitor-cephfs-secret \
    --from-literal=userKey=AQBNGW5fePrMDhAAmZJpoeZ40VjODlprwyMjMg== \
    --from-literal=userID=estid
