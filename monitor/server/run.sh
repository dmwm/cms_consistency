#!/bin/bash

prefix=cmscon
app=monitor

if [ "$1" == "shell" ]; then
	docker run --rm --name ${prefix}-$app \
		-ti \
		-v `pwd`/samples:/samples \
		-p 8888:8400 \
		${prefix}-$app /bin/bash

else
	docker run --rm  --name ${prefix}-$1 \
		-d \
		-v `pwd`/samples:/samples \
		-p 8888:8400 \
		${prefix}-$app
fi

