#FROM centos/python-38-centos7 
FROM conda/miniconda3-centos7

RUN conda update -n base -c defaults conda && \
    conda clean

RUN pip install webpie pythreader jinja2 && \
    rm -rf ~/.cache/pip

WORKDIR /root

ADD start.sh ./start.sh
RUN mkdir app samples
ADD app ./app

EXPOSE 8400

ENTRYPOINT ["/root/start.sh"]




