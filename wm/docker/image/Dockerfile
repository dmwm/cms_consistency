FROM centos:7
USER root

RUN yum upgrade -y \
    && yum clean all \
    && rm -rf /var/cache/yum


RUN yum install -y git curl which\
    && yum clean all \
    && rm -rf /var/cache/yum


RUN yum install -y epel-release.noarch\
    && yum clean all \
    && rm -rf /var/cache/yum


# PKI stuff
RUN yum install -y https://repo.opensciencegrid.org/osg/3.5/osg-3.5-el7-release-latest.rpm\
    && yum clean all \
    && rm -rf /var/cache/yum

RUN yum install -y osg-pki-tools\
    && yum clean all \
    && rm -rf /var/cache/yum

#RUN rpm -i http://mirror.grid.uchicago.edu/pub/osg/3.3/el7/testing/x86_64/voms-2.0.14-1.3.osg33.el7.x86_64.rpm
RUN rpm -i http://mirror.grid.uchicago.edu/pub/osg/3.5/el7/release/x86_64/voms-2.0.14-1.6.osg35.el7.x86_64.rpm\
    && yum clean all \
    && rm -rf /var/cache/yum

RUN rpm -i http://mirror.grid.uchicago.edu/pub/osg/3.5/el7/release/x86_64/voms-clients-cpp-2.0.14-1.6.osg35.el7.x86_64.rpm\
    && yum clean all \
    && rm -rf /var/cache/yum

RUN yum install -y osg-ca-certs\
    && yum clean all \
    && rm -rf /var/cache/yum


COPY vomses /etc

# Oracle client
RUN yum install -y libaio\
    && yum clean all \
    && rm -rf /var/cache/yum

RUN rpm -i https://download.oracle.com/otn_software/linux/instantclient/19600/oracle-instantclient19.6-basic-19.6.0.0.0-1.x86_64.rpm

# Python packages
RUN yum install -y python3 python3-pip\
    && yum clean all \
    && rm -rf /var/cache/yum

RUN pip3  --no-cache-dir install SQLAlchemy pyyaml pythreader cx_Oracle

# xrootd client
RUN curl -o /etc/yum.repos.d/xrootd-stable-slc7.repo https://xrootd.slac.stanford.edu/binaries/xrootd-stable-slc7.repo
RUN yum install -y xrootd-libs xrootd-client\
    && yum clean all \
    && rm -rf /var/cache/yum

RUN useradd -ms /bin/bash rucio

RUN ls -l /home

WORKDIR /home/rucio
RUN echo 1
RUN git clone https://github.com/ivmfnal/cms_consistency.git
RUN cd cms_consistency; git checkout wm-scanner

COPY run.sh .

CMD /bin/bash


