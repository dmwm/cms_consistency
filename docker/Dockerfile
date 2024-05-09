FROM almalinux:9
USER root

RUN dnf install -y epel-release.noarch\
    && dnf clean all \
    && rm -rf /var/cache/dnf

# PKI stuff
RUN dnf install -y https://repo.opensciencegrid.org/osg/3.6/osg-3.6-el9-release-latest.rpm\
    && dnf clean all \
    && rm -rf /var/cache/dnf

RUN dnf install -y osg-pki-tools\
    && dnf clean all \
    && rm -rf /var/cache/dnf

RUN dnf install -y voms\
    && dnf clean all \
    && rm -rf /var/cache/dnf

#RUN rpm -i http://mirror.grid.uchicago.edu/pub/osg/3.5/el7/release/x86_64/voms-clients-cpp-2.0.14-1.6.osg35.el7.x86_64.rpm\
RUN  dnf install -y voms-clients-cpp\
    && dnf clean all \
    && rm -rf /var/cache/dnf

RUN mkdir -p /etc/grid-security \
    && rm -rf /etc/grid-security/certificates \
    && ln -s /cvmfs/grid.cern.ch/etc/grid-security/certificates /etc/grid-security/

# Oracle client
RUN dnf install -y libaio\
    && dnf clean all \
    && rm -rf /var/cache/dnf

#RUN rpm -i https://download.oracle.com/otn_software/linux/instantclient/19600/oracle-instantclient19.6-basic-19.6.0.0.0-1.x86_64.rpm
COPY oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm /tmp
RUN dnf install -y /tmp/oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm

#.. xrootd client
#RUN curl -o /etc/dnf.repos.d/xrootd-stable-slc7.repo https://xrootd.slac.stanford.edu/binaries/xrootd-stable-slc7.repo
RUN dnf install -y xrootd-libs xrootd-client\
    && dnf clean all \
    && rm -rf /var/cache/dnf

# jobber
RUN rpm -i https://github.com/dshearer/jobber/releases/download/v1.4.4/jobber-1.4.4-1.el8.x86_64.rpm

# Python and libs
RUN dnf install -y python3 python3-pip git \
    && dnf clean all \
    && rm -rf /var/cache/dnf

RUN pip3 install --upgrade pip
RUN pip3 install --upgrade setuptools
RUN pip3 --no-cache-dir install SQLAlchemy pyyaml pythreader cx_Oracle j2cli
RUN pip3 install rucio-clients rucio-consistency

RUN mkdir -p /consistency
RUN mkdir /root/RAL
COPY vomses /etc
COPY cleanup.sh run.sh site.sh unmerged_site.sh RAL_Disk_pre.sh RAL_Disk_post.sh  RAL_Tape_pre.sh RAL_Tape_post.sh rucio-client.cfg /consistency/

ADD rucio.cfg.j2 /tmp

WORKDIR /consistency
RUN chmod +x *.sh

RUN git clone https://github.com/dmwm/cms_consistency.git

CMD /bin/bash