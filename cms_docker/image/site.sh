#!/bin/bash

cd /consistency/cms_consistency/site_cmp3

PYTHON=python3 \            
    ./site_cmp3.sh \
        /config/config.yaml \              
        /opt/rucio/etc/rucio.cfg \
        $1  \                               # RSE name in rucio DB and in the config.yaml
        /var/cache/consistency-temp \       # scratch area
        /var/cache/consistency-dump \       # output area
        /opt/proxy/x509up                   
