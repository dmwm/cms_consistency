#!/bin/bash

prefix=cmscon
app=monitor

if [ "$1" == "shell" ]; then
	docker run --rm --name ${prefix}-$app \
		-ti \
		-v `pwd`/real-samples:/samples \
		-p 8888:9093 \
		${prefix}-$app /bin/bash

else
	docker run --rm  --name ${prefix}-$1 \
		-d \
		-v `pwd`/real-samples:/samples \
		-p 8888:9093 \
		${prefix}-$app
fi

