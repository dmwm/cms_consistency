#!/bin/sh

set -e

export CONSISTENCY_VERSION=4.8.7

export HARBOR=registry.cern.ch/cmsrucio

podman build -t $HARBOR/rucio-consistency:release-$CONSISTENCY_VERSION .
podman push $HARBOR/rucio-consistency:release-$CONSISTENCY_VERSION
