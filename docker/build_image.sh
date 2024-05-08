#!/bin/sh

set -e

export CONSISTENCY_VERSION=4.5.0

export HARBOR=registry.cern.ch/cmsrucio

podman build -t $HARBOR/rucio-consistency:release-$CONSISTENCY_VERSION .
podman push $HARBOR/rucio-consistency:release-$CONSISTENCY_VERSION
