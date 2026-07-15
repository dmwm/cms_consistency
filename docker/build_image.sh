#!/bin/sh

export CONSISTENCY_VERSION=4.9.0 # Make it 5.0.0 when it works

export HARBOR=registry.cern.ch/cmsrucio

podman build -t $HARBOR/rucio-consistency:release-$CONSISTENCY_VERSION .
podman push $HARBOR/rucio-consistency:release-$CONSISTENCY_VERSION
