#!/bin/bash

prefix=cmscon
app=monitor

if [ "$1" == "shell" ]; then
	docker run --rm --name appsrv-$app \
		-ti \
		-v /storage/local/data1/ivm/logs:/home/appsrv/logs \
		-p 9093:9093 \
		${prefix}-$app /bin/bash

else
	docker run --rm  --name appsrv-$1 \
		-d \
		-v /storage/local/data1/ivm/logs:/home/appsrv/logs \
		-p 9093:9093 \
		${prefix}-$app
fi

