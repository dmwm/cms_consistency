FROM conda/miniconda2-centos7
USER root

# PKI stuff
RUN yum install -y https://repo.opensciencegrid.org/osg/3.5/osg-3.5-el7-release-latest.rpm
RUN yum install -y osg-pki-tools
RUN rpm -i http://mirror.grid.uchicago.edu/pub/osg/3.3/el7/testing/x86_64/voms-2.0.14-1.3.osg33.el7.x86_64.rpm
RUN rpm -i http://mirror.grid.uchicago.edu/pub/osg/3.3/el7/testing/x86_64/voms-clients-cpp-2.0.14-1.3.osg33.el7.x86_64.rpm
RUN yum install -y osg-ca-certs

COPY vomses /etc


# Oracle client
RUN yum install -y libaio
RUN rpm -i https://download.oracle.com/otn_software/linux/instantclient/19600/oracle-instantclient19.6-basic-19.6.0.0.0-1.x86_64.rpm
RUN pip install SQLAlchemy pyyaml pythreader #cx_Oracle

# xrootd client
RUN curl -o /etc/yum.repos.d/xrootd-stable-slc7.repo https://xrootd.slac.stanford.edu/binaries/xrootd-stable-slc7.repo
RUN yum install -y xrootd-libs xrootd-client

RUN yum install -y git

RUN useradd -ms /bin/bash rucio

RUN ls -l /home

USER rucio
WORKDIR /home/rucio

RUN git clone https://github.com/ivmfnal/cms_consistency.git

WORKDIR /home/rucio

CMD /bin/bash


