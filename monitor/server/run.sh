#!/bin/bash

prefix=rucio-con
app=mon

if [ "$1" == "shell" ]; then
	docker run --rm --name ${prefix}-$app \
		-ti \
		-v `pwd`/samples:/cc_data -v /storage/local/data1/ivm/wm:/wm_data\
		-p 8888:8400 \
		${prefix}-$app /bin/bash

else
	docker run --rm  --name ${prefix}-$1 \
		-d \
		-v `pwd`/samples:/cc_data -v /storage/local/data1/ivm/wm:/wm_data\
		-p 8888:8400 \
		${prefix}-$app
fi

