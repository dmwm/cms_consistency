#!/bin/bash 

tag=$1

docker tag rucio-con-mon ivmfnal/rucio_consistency_monitor:$tag
docker push ivmfnal/rucio_consistency_monitor:$tag

