#!/bin/bash

prefix=cmscon

if [ "$2" == "shell" ]; then
	docker run --rm --name appsrv-$1 \
		-ti \
		-v /storage/local/data1/ivm/logs:/home/appsrv/logs \
		-p 9093:9093 \
		${prefix}-$1 /bin/bash

else
	docker run --rm  --name appsrv-$1 \
		-d \
		-v /storage/local/data1/ivm/logs:/home/appsrv/logs \
		-p 9093:9093 \
		${prefix}-$1 
fi

