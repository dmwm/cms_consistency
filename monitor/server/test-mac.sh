#!/bin/bash
# File: test-mac.sh
# Purpose:
#   Launch a podman image locally on a MacBook for locally testing purposes.

#.. delete previous image
imageID=`podman images | grep rucio-con-mon | awk '{print $3}'`
test -n $imageID && podman rmi $imageID

#.. run updated image
podman run --rm -it \
       -p 8080:8400 \
       --env http_proxy=$http_proxy --env https_proxy=$https_proxy \
       registry.cern.ch/cmsrucio/rucio-con-mon:2.1.3 \
       /bin/bash --login
