#!/bin/bash 

docker tag rucio-con-mon ivmfnal/rucio_consistency_monitor:latest
docker tag rucio-con-mon ivmfnal/rucio_consistency_monitor:2.2
docker push ivmfnal/rucio_consistency_monitor:latest
docker push ivmfnal/rucio_consistency_monitor:2.2

