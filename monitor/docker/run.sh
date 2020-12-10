#!/bin/bash

if [ "$2" == "shell" ]; then
	docker run --rm --name appsrv-$1 \
		-ti \
		-v /storage/local/data1/ivm/logs:/home/appsrv/logs \
		-p 9093:9093 \
		appsrv-$1 shell

else
	docker run --rm  --name appsrv-$1 \
		-d \
		-v /storage/local/data1/ivm/logs:/home/appsrv/logs \
		-p 9093:9093 \
		appsrv-$1 
fi

