FROM almalinux:9
USER root

RUN dnf upgrade -y \
    && dnf clean all \
    && rm -rf /var/cache/dnf

RUN dnf install -y git which\
    && dnf clean all \
    && rm -rf /var/cache/dnf

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
#    && dnf clean all \
#    && rm -rf /var/cache/dnf

RUN dnf install -y voms-clients-cpp \
    && dnf clean all \
    && rm -rf /var/cache/dnf

RUN dnf install -y osg-ca-certs\
    && dnf clean all \
    && rm -rf /var/cache/dnf

COPY vomses /etc

# Oracle client
RUN dnf install -y libaio\
    && dnf clean all \
    && rm -rf /var/cache/dnf

#RUN rpm -i https://download.oracle.com/otn_software/linux/instantclient/2112000/el9/oracle-instantclient6-basic-21.12.0.0.0-1.el9.x86_64.rpm
#RUN dnf install -y oracle-release-el9
#RUN dnf install -y oracle-instantclient-release-el9
#RUN dnf install -y oracle-instantclient-basic
COPY oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm /tmp
RUN dnf install -y /tmp/oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm


# Python packages
RUN dnf install -y python3 python3-pip\
    && dnf clean all \
    && rm -rf /var/cache/dnf

RUN pip3  --no-cache-dir install SQLAlchemy pyyaml pythreader cx_Oracle

#.. xrootd client
RUN dnf install -y xrootd-libs xrootd-client\
    && dnf clean all \
    && rm -rf /var/cache/dnf

RUN useradd -ms /bin/bash rucio

RUN ls -l /home

WORKDIR /home/rucio
RUN echo 1
RUN git clone https://github.com/dmwm/cms_consistency.git
RUN cd cms_consistency

COPY run.sh .

CMD /bin/bash
