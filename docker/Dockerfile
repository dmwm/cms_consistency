FROM almalinux:9
USER root

RUN dnf -y update \
    && dnf install -y epel-release.noarch \
    && dnf clean all \
    && rm -rf /var/cache/dnf

RUN dnf install -y https://repo.opensciencegrid.org/osg/25-main/osg-25-main-el9-release-latest.rpm && \
    dnf install -y osg-pki-tools voms voms-clients-cpp \
      xrootd-libs xrootd-client \
      davix python-gfal2 gfal2-plugin-file gfal2-plugin-xrootd gfal2-plugin-http gfal2-util-scripts \
      python3 python3-pip \
      git which diffutils cronie-noanacron libaio procps-ng && \
    dnf clean all && \
    rm -rf /var/cache/dnf

#.. Oracle client
# depends on libaio
RUN rpm -i https://yum.oracle.com/repo/OracleLinux/OL8/oracle/instantclient21/x86_64/getPackage/oracle-instantclient-basic-21.20.0.0.0-1.el8.x86_64.rpm

#.. jobber -- obsolete, soon to be removed
RUN rpm -i https://github.com/dshearer/jobber/releases/download/v1.4.4/jobber-1.4.4-1.el8.x86_64.rpm

#.. crypto policies
RUN update-crypto-policies --set DEFAULT:SHA1

#.. pip3-installed stuff
RUN pip3 install --upgrade pip
RUN pip3 --no-cache-dir install SQLAlchemy pyyaml pythreader cx_Oracle j2cli
RUN pip3 install --no-cache-dir --pre rucio[oracle,mysql,postgresql]
RUN pip3 install rucio-clients rucio-consistency

# grid certs
RUN mkdir -p /etc/grid-security && \
    rm -rf /etc/grid-security/certificates && \
    ln -s /cvmfs/grid.cern.ch/etc/grid-security/certificates /etc/grid-security/

### Local setup
RUN mkdir -p /consistency
RUN mkdir /root/RAL
COPY vomses /etc

COPY cleanup.sh run.sh site.sh unmerged_site.sh RAL_Disk_pre.sh RAL_Disk_post.sh RAL_Tape_pre.sh RAL_Tape_post.sh rucio-client.cfg push2prometheus.py /consistency/

ADD rucio.cfg.j2 /tmp
ADD DeterministicDelay /

WORKDIR /consistency
RUN chmod +x *.sh

RUN git clone https://github.com/dmwm/cms_consistency.git

# FIXME: Move location of davs_scanner to rucio_consistency when accepted.  Remove reference to config.py
# FIXME: Add several files that we need to change. Eventually to be upstreamed
ADD https://raw.githubusercontent.com/ericvaandering/davix-explore/refs/heads/main/rucio_consistency/config.py /usr/local/lib/python3.9/site-packages/rucio_consistency/config.py
ADD https://raw.githubusercontent.com/ericvaandering/consistency-enforcement/refs/heads/for_cms/davs_scanner.py /consistency/cms_consistency/site_cmp3/davs_scanner.py

RUN chmod +x /consistency/cms_consistency/site_cmp3/davs_scanner.py

CMD /bin/bash
