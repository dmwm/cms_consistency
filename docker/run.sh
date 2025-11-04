#!/bin/sh

# Assumptions:
#   X.509 proxy is in /opt/proxy/x509up
#   rucio.cfg is in /opt/rucio/etc/rucio.cfg
#     or needs to be generated from template /tmp/rucio.cfg.j2
#   scanned config.yaml in /config/config.yaml
#   jobber config in /etc/jobber-config/dot-jobber.yaml
#     or
#   crontab is in /etc/cron.rucio/consistency-crontab
#

if [ ! -f /config/config.yaml ]; then
    echo /config/config.yaml not found
    exit 1
fi

if [ ! -f /opt/proxy/x509up ]; then
    echo /opt/proxy/x509up not found
    exit 1
fi

if [ ! -f /opt/rucio/etc/rucio.cfg ]; then
    if [ ! -f /tmp/rucio.cfg.j2 ]; then
        echo /opt/rucio/etc/rucio.cfg nor /tmp/rucio.cfg.j2 could be found
        exit 1
    fi
    mkdir -p /opt/rucio/etc
    echo Generating rucio.cfg from the template
    j2 /tmp/rucio.cfg.j2 | sed '/^\s*$/d' > /opt/rucio/etc/rucio.cfg
fi

if [ ! -d /var/cache/consistency-dump ]; then
    echo Output directory /var/cache/consistency-dump not mounted
    exit 1
fi

if [ ! -d /var/cache/consistency-temp ]; then
    echo Scratch directory /var/cache/consistency-temp not mounted
    exit 1
fi

cp /config/config.yaml /consistency/config.yaml

cd /consistency/cms_consistency
git pull				# make sure to pick up the latest version

if [ ! -z "$RUCIO_USING_CRON" ]; then
  echo "Setting up and starting cron"
  cp /etc/cron.rucio/consistency-crontab /etc/cron.d/
  crond -n -s
else
  if [ ! -f /etc/jobber-config/dot-jobber.yaml ]; then
    echo /etc/jobber-config/dot-jobber.yaml not found
    exit 1
  fi

  cp /etc/jobber-config/dot-jobber.yaml /root/.jobber

  echo "Not using cron. Starting Jobber"
  /usr/local/libexec/jobbermaster &

  sleep 5

  echo
  echo "============= Jobber log file ============="

  tail -f /var/log/jobber-runs
fi
