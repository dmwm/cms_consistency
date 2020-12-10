#FROM centos/python-38-centos7 
FROM conda/miniconda3-centos7

RUN conda update -n base -c defaults conda

RUN yum install -y git make curl && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN pip install webpie pythreader

WORKDIR /root
RUN git clone https://github.com/ivmfnal/cms_consistency.git
RUN mkdir bin
COPY bin bin/




