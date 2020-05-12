FROM conda/miniconda2-centos7
USER root
RUN yum install -y libaio
RUN rpm -i https://download.oracle.com/otn_software/linux/instantclient/19600/oracle-instantclient19.6-basic-19.6.0.0.0-1.x86_64.rpm
RUN pip install SQLAlchemy cx_Oracle
RUN yum install -y git

RUN useradd -ms /bin/bash rucio

RUN ls -l /home

USER rucio
WORKDIR /home/rucio

RUN git clone https://github.com/ivmfnal/cms_consistency.git

WORKDIR /home/rucio/cms_consistency/direct

CMD /bin/bash


